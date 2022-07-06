package lib

import java.util.UUID
import scala.util.{Failure, Success, Try}

/** Pretend database that lends access to a lib.DbSession or lib.DbTransaction
  * for mock read/write database operations
  */
trait DB {
	def withSession[A](label: String)(work: DbSession => A): A
	def withTransaction[A](label: String)(work: DbTransaction => A): A
}
object DB extends DB {
	def withSession[A](label: String)(work: DbSession => A): A = {
		val s = new DbSession(UUID.randomUUID)
		println(s"start session ${s.id} for $label")
		try work(s)
		finally s.close()
	}
	def withTransaction[A](label: String)(work: DbTransaction => A): A = {
		withSession(label) { _.withTransaction(work) }
	}
}

/** Pretend database session, used to run mock SQL queries.
  */
class DbSession(val id: UUID) {
	def close(): Unit = println(s"close session $id")
	def withTransaction[A](work: DbTransaction => A): A = {
		val tx = new DbTransaction(id)
		println(s"transaction start in $id")
		val result = Try { work(tx) }
		result match {
			case Success(a) =>
				tx.commit()
				a
			case Failure(err) =>
				tx.rollback()
				throw err
		}
	}
	override def toString = s"lib.DbSession($id)"
}

/** Pretend database transaction, used to run mock SQL updates.
  * A session can be temporarily upgraded to a transaction using
  * its `withTransaction` method.
  */
class DbTransaction(id: UUID) extends DbSession(id) {
	def commit(): Unit = println(s"commit tx $id")
	def rollback(): Unit = println(s"rollback tx $id")
	override def toString = s"lib.DbTransaction($id)"
}
