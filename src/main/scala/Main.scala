import cats.Monad
import cats.syntax.show.*
import cats.syntax.traverse.*
import example.{MockDbProjectAccess, ProjectAccess}
import lib.{DB, LazyQuery}
import model.*

import scala.util.control.{NonFatal, TailCalls}

object Main extends App {

	def skip[A](f: => A): Unit = ()

	skip {
		val q1 = for {
			i <- LazyQuery.delay {100}
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
		catch {case NonFatal(e) => e.printStackTrace()}
	}

	skip {
		val q2 = for {
			i <- LazyQuery.delay {10}
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
	}

	skip {
		val t = for {
			i <- TailCalls.tailcall(TailCalls.done(100))
			j <- TailCalls.tailcall {TailCalls.done((0 to i).toList)}
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
		catch {case NonFatal(e) => e.printStackTrace()}
	}

	skip {
		val q3 = Monad[LazyQuery].tailRecM(1000) {
			case 0 => LazyQuery.pure(Right("done"))
			case i =>
				val stackDepth = Thread.currentThread.getStackTrace.length
				println(s"Update $i @ stack depth $stackDepth")
				if (i > 990) LazyQuery.pure(Left(i - 1))
				else if (i > 880) LazyQuery.read("db read") { s => Left(i - 1) }
				else if (i > 770) LazyQuery.write("db write") { tx => Left(i - 1) }
				else if (i > 50) LazyQuery.pure(Left(i - 1))
				else throw new Exception("crash plz")
		}
		try q3.runStateless(DB)
		catch {case NonFatal(e) => e.printStackTrace()}
	}

	skip {
		def recurse(i: Int): LazyQuery[String] = LazyQuery.pure(i).flatMap {
			case 0 => LazyQuery.pure("done")
			case i =>
				val stackDepth = Thread.currentThread.getStackTrace.length
				println(s"Update $i @ stack depth $stackDepth")
				if (i > 990) LazyQuery.pure(i - 1).flatMap(recurse)
				else if (i > 880) LazyQuery.read("db read") { s => i - 1 }.flatMap(recurse)
				else if (i > 770) LazyQuery.write("db write") { tx => i - 1 }.flatMap(recurse)
				else if (i > 50) LazyQuery.pure(i - 1).flatMap(recurse)
				else throw new Exception("crash plz")
		}

		val q4 = recurse(1000)
		try q4.runStateless(DB)
		catch {case NonFatal(e) => e.printStackTrace()}
	}

	skip {
		val q5 = for {
			i <- LazyQuery.pure("hello")
			j <- LazyQuery.read("R") { _ => 123 }
			k <- LazyQuery.write("W") { _ => true }
			l <- LazyQuery.delay {List(1, 2, 3)}
			m <- LazyQuery.write("W2") { _ => 1234L }
		} yield (i + " world", -j, !k, l.map(_ + 1), m + 5)
		q5.runStateless(DB)
	}

	skip {
		val projects: ProjectAccess[LazyQuery] = new MockDbProjectAccess
		val program = for {
			p1 <- projects.insert(Project("Hello")).leftMap(_.toErrorMessage)
			a <- projects.startAnalysis(p1).leftMap(_.toErrorMessage)
			_ <- projects.delete(p1).leftMap(_.toErrorMessage)
		} yield ()
		println(program.value.runStateless(DB))
	}

	skip {
		val projects: ProjectAccess[LazyQuery] = new MockDbProjectAccess
		val program = for {
			p1 <- projects.insert(Project("Hello")).leftMap(_.toErrorMessage)
			a <- projects.startAnalysis(p1).leftMap(_.toErrorMessage)
			_ <- projects.finishAnalysis(a, isSuccess = true).leftMap(_.toErrorMessage)
			_ <- projects.delete(p1).leftMap(_.toErrorMessage)
		} yield ()
		println(program.value.runStateless(DB))
	}

	skip {
		val projects: ProjectAccess[LazyQuery] = new MockDbProjectAccess
		val program = for {
			p1 <- projects.insert(Project("hello")).rethrowT
			a <- projects.startAnalysis(p1).rethrowT
			_ <- projects.delete(p1).rethrowT.handleErrorWith { e =>
				println(s"Caught error in project delete: ${e}")
				println("finishing analysis instead")
				projects.finishAnalysis(a, isSuccess = true).rethrowT
			}
		} yield ()
		println(program.runStateless(DB))
	}

	locally {

		def makeProgram = {
			val projects: ProjectAccess[LazyQuery] = new MockDbProjectAccess

			// insert a project, then throw an exception
			val p1 = for {
				p <- projects.insert(Project("hello")).rethrowT
				_ <- LazyQuery.raiseError {new Exception("oh no")}
			} yield p

			// note the current transaction state, then insert a different project
			val p2 = for {
				_ <- LazyQuery.pure(()).withSideEffect { (_, tx) =>
					println(s"Starting P2 with state $tx")
				}
				p <- projects.insert(Project("hello2")).rethrowT
			} yield p

			// run p1, catch the error, continue with p2
			p1.handleErrorWith { err =>
				println(s"recovered from $err, continue with backup program")
				p2
			}
		}

		// in this one, the error recovery scope starts before the transaction starts,
		// so when the exception is thrown, the transaction ends before the exception
		// is caught. So when `p2` starts, `withSideEffect` observes there is no
		// transaction, and the next insert starts a new transaction.
		println("------NON TRANSACTIONAL PROGRAM START------")
		println(makeProgram.runStateless(DB))
		println("------END------\n\n")

		// in this one, calling `.transactional` makes the entire program want to run
		// in a transaction, and so the error recovery logic occurs inside that transaction.
		// So the transaction is not interrupted by the throw and catch, so when `p2`
		// starts, it runs inside the original transaction.
		println("------TRANSACTIONAL PROGRAM START------")
		println(makeProgram.transactional("program!").runStateless(DB))
		println("------END------")
	}

	println("Done")
}
