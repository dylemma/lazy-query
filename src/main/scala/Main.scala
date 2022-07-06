import cats.Monad
import cats.syntax.traverse.*
import cats.syntax.show.*
import example.{MockDbProjectAccess, ProjectAccess}
import lib.{DB, LazyQuery}
import model._

import scala.util.control.{NonFatal, TailCalls}

object Main extends App {

	val q1 = for {
		i <- LazyQuery.delay { 100 }
		j <- LazyQuery.read("j") { s => (0 to i).toList }
		j2 <- j.traverse { n =>
			LazyQuery.write("update") { tx =>
				val stackDepth = Thread.currentThread.getStackTrace.length
				println(s"Update $n @ stack depth $stackDepth")
				if (n == 50) throw new Exception("show me stack")
				n * 2
			}
		}
	} yield j2
	try q1.runStateless(DB)
	catch { case NonFatal(e) => e.printStackTrace() }

	val q2 = for {
		i <- LazyQuery.delay { 10 }
		j <- LazyQuery.read("j") { s => (0 to i).toList }
		j2 <- j.traverse { n =>
			LazyQuery.write("update") { tx =>
				val stackDepth = Thread.currentThread.getStackTrace.length
				println(s"Update $n @ stack depth $stackDepth")
				n * 2
			}
		}
	} yield j2
	q2.runStateless(DB)

	val t = for {
		i <- TailCalls.tailcall(TailCalls.done(100))
		j <- TailCalls.tailcall { TailCalls.done((0 to i).toList) }
		j2 <- j.traverse { n =>
			TailCalls.tailcall {
				val stackDepth = Thread.currentThread.getStackTrace.length
				println(s"Update $n @ stack depth $stackDepth")
				if (n == 50) throw new Exception("show me stack")
				TailCalls.done(n * 2)
			}
		}
	} yield j2
	try t.result
	catch { case NonFatal(e) => e.printStackTrace() }

	val q3 = Monad[LazyQuery].tailRecM(1000) {
		case 0 => LazyQuery.pure(Right("done"))
		case i =>
			val stackDepth = Thread.currentThread.getStackTrace.length
			println(s"Update $i @ stack depth $stackDepth")
			if (i > 990) LazyQuery.pure(Left(i - 1))
			else if (i > 880) LazyQuery.read("db read") { s =>  Left(i - 1) }
			else if (i > 770) LazyQuery.write("db write") { tx => Left(i - 1) }
			else if (i > 50) LazyQuery.pure(Left(i - 1))
			else throw new Exception("crash plz")
	}
	try q3.runStateless(DB)
	catch { case NonFatal(e) => e.printStackTrace() }

	def recurse(i: Int): LazyQuery[String] = LazyQuery.pure(i).flatMap {
		case 0 => LazyQuery.pure("done")
		case i =>
			val stackDepth = Thread.currentThread.getStackTrace.length
			println(s"Update $i @ stack depth $stackDepth")
			if (i > 990) LazyQuery.pure(i - 1).flatMap(recurse)
			else if (i > 880) LazyQuery.read("db read") { s =>  i - 1 }.flatMap(recurse)
			else if (i > 770) LazyQuery.write("db write") { tx => i - 1 }.flatMap(recurse)
			else if (i > 50) LazyQuery.pure(i - 1).flatMap(recurse)
			else throw new Exception("crash plz")
	}
	val q4 = recurse(1000)
	try q4.runStateless(DB)
	catch { case NonFatal(e) => e.printStackTrace() }

	val q5 = for {
		i <- LazyQuery.pure("hello")
		j <- LazyQuery.read("R") { _ => 123 }
		k <- LazyQuery.write("W") { _ => true }
		l <- LazyQuery.delay { List(1,2,3) }
		m <- LazyQuery.write("W2") { _ => 1234L }
	} yield (i + " world", -j, !k, l.map(_ + 1), m + 5)
	q5.runStateless(DB)

	locally {
		val projects: ProjectAccess[LazyQuery] = new MockDbProjectAccess
		val program = for {
			p1 <- projects.insert(Project("Hello")).leftMap(_.toErrorMessage)
			a <- projects.startAnalysis(p1).leftMap(_.toErrorMessage)
			_ <- projects.delete(p1).leftMap(_.toErrorMessage)
		} yield ()
		println(program.value.runStateless(DB))
	}

	locally {
		val projects: ProjectAccess[LazyQuery] = new MockDbProjectAccess
		val program = for {
			p1 <- projects.insert(Project("Hello")).leftMap(_.toErrorMessage)
			a <- projects.startAnalysis(p1).leftMap(_.toErrorMessage)
			_ <- projects.finishAnalysis(a, isSuccess = true).leftMap(_.toErrorMessage)
			_ <- projects.delete(p1).leftMap(_.toErrorMessage)
		} yield ()
		println(program.value.runStateless(DB))
	}

	println("Done")
}
