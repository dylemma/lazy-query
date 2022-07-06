package example

import cats.data.{EitherT, OptionT}
import cats.syntax.show.*
import cats.{Show, ~>}
import lib.LazyQuery
import model.*

import scala.annotation.targetName

object ProjectAccess {
	sealed trait Error extends Exception {
		def toErrorMessage: String
		override def getMessage: String = toErrorMessage
	}
	object Error {
		implicit val showCheckedError: Show[Error] = _.toErrorMessage
	}

	case class NoSuchProject(id: ProjectId) extends Error {
		def toErrorMessage = s"No project exists with id ${id.value}"
	}
	case class NoSuchAnalysis(id: AnalysisId) extends Error {
		def toErrorMessage = s"No analysis exists with id ${id.value}"
	}
	case class NameConflict(name: String, existingId: ProjectId) extends Error {
		def toErrorMessage = s"Project ${existingId.value} already exists with the name '$name'"
	}
	case class AnalysisInProgress(id: AnalysisId, context: ProjectId) extends Error {
		def toErrorMessage = s"Analysis ${id.value} is already in progress in project ${context.value}"
	}
}

trait ProjectAccess[F[_]] { self =>
	import ProjectAccess.*

	def get(id: ProjectId): OptionT[F, Project]
	def insert(p: Project): EitherT[F, NameConflict, ProjectId]
	def update(id: ProjectId, state: Project): EitherT[F, NoSuchProject, Unit]
	def delete(id: ProjectId): EitherT[F, NoSuchProject | AnalysisInProgress, Unit]
	def startAnalysis(context: ProjectId): EitherT[F, NoSuchProject | AnalysisInProgress, AnalysisId]
	def finishAnalysis(id: AnalysisId, isSuccess: Boolean): EitherT[F, NoSuchAnalysis, Unit]
	def all: F[List[ProjectId #: Project]]

	def mapK[G[_]](f: F ~> G): ProjectAccess[G] = new ProjectAccess[G] :
		def get(id: ProjectId) = self.get(id).mapK(f)
		def insert(p: Project) = self.insert(p).mapK(f)
		def update(id: ProjectId, state: Project) = self.update(id, state).mapK(f)
		def delete(id: ProjectId) = self.delete(id).mapK(f)
		def startAnalysis(context: ProjectId) = self.startAnalysis(context).mapK(f)
		def finishAnalysis(id: AnalysisId, isSuccess: Boolean) = self.finishAnalysis(id, isSuccess).mapK(f)
		def all: G[List[ProjectId #: Project]] = f(self.all)
}

class MockDbProjectAccess extends ProjectAccess[LazyQuery] {
	import ProjectAccess.*
	private case class ProjectState(name: String, analysis: Option[AnalysisId])
	private val projectIdGen = Iterator.from(1).map(ProjectId.apply)
	private val analysisIdGen = Iterator.from(1).map(AnalysisId.apply)
	private val store = collection.mutable.Map.empty[ProjectId, ProjectState]

	def get(id: ProjectId): OptionT[LazyQuery, Project] = OptionT(LazyQuery.read(s"get $id") { dbSession =>
		store.get(id).map(s => Project(s.name))
	})

	def insert(p: Project): EitherT[LazyQuery, NameConflict, ProjectId] = EitherT(LazyQuery.write(s"create $p") { tx =>
		store.find(_._2.name == p.name) match {
			case Some((id, state)) => Left(NameConflict(state.name, id))
			case None =>
				val id = projectIdGen.next()
				store.put(id, ProjectState(p.name, analysis = None))
				Right(id)
		}
	})

	def update(id: ProjectId, state: Project): EitherT[LazyQuery, NoSuchProject, Unit] = EitherT(LazyQuery.write(s"update $id to $state") { tx =>
		store
			.updateWith(id)(_.map { s => s.copy(name = state.name) })
			.map(_ => ())
			.toRight { NoSuchProject(id) }
	})

	def delete(id: ProjectId): EitherT[LazyQuery, NoSuchProject | AnalysisInProgress, Unit] = EitherT(LazyQuery.write(s"delete $id") { tx =>
		store.get(id) match {
			case None => Left(NoSuchProject(id))
			case Some(ProjectState(_, Some(analysisId))) => Left(AnalysisInProgress(analysisId, id))
			case Some(_) =>
				store.remove(id)
				Right(())
		}
	})

	def startAnalysis(context: ProjectId): EitherT[LazyQuery, NoSuchProject | AnalysisInProgress, AnalysisId] = EitherT(LazyQuery.write(s"start analysis in $context") { tx =>
		store.get(context) match {
			case None => Left(NoSuchProject(context))
			case Some(ProjectState(_, Some(analysisId))) => Left(AnalysisInProgress(analysisId, context))
			case Some(s) =>
				val analysisId = analysisIdGen.next()
				store.put(context, s.copy(analysis = Some(analysisId)))
				Right(analysisId)
		}
	})

	def finishAnalysis(id: AnalysisId, isSuccess: Boolean): EitherT[LazyQuery, NoSuchAnalysis, Unit] = EitherT(LazyQuery.write(s"finish $id") { tx =>
		store.find(_._2.analysis.contains(id)) match {
			case None => Left(NoSuchAnalysis(id))
			case Some((pid, state)) =>
				store.put(pid, state.copy(analysis = None))
				Right(())
		}
	})

	def all: LazyQuery[List[ProjectId #: Project]] = LazyQuery.read("all projects") { dbSession =>
		store.view.map { case (pid, state) => pid #: Project(state.name) }.toList.sortBy(_.id.value)
	}
}