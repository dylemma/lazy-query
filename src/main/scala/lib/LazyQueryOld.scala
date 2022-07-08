package lib

import cats.syntax.applicative.*
import cats.{Applicative, Monad, MonadError, StackSafeMonad, ~>, Defer as CatsDefer}

import scala.annotation.tailrec

/** Monad for synchronous database access that abstracts over a  `db.withSession { session => ??? }`
  * or  `db.withTransaction { tx => ??? }` connection lender pattern.
  *
  * Execution of the monad will lazily upgrade from "no session" to "session" to "transaction"
  * on demand, preferring to remain in a "no session" state until one is required. Once a session
  * is requested, one will be opened and remain open until execution completes. Similarly, once
  * a transaction is requested, one will be opened and remain open until execution completes.
  * A session can be "upgraded" to a transaction, but a transaction will not be downgarded to session.
  *
  * `lib.LazyQuery` is stack-safe.
  *
  * @tparam A the result type
  */
sealed trait LazyQueryOld[+A] {
	import LazyQueryOld._

	/* Implementation note:
	 * The Done/Call/Cont classes are essentially a copy of scala.util.control.TailCalls,
	 * with the goal of stack-safety in the `flatMap` and `tailRecM` monad methods.
	 * Read and Write are like Call, but act as a gate that prompts the runner to open
	 * a session or transaction if one isn't currently in use.
	 */

	@tailrec final def runStateless(db: DB): A = this match {
		case Done(a) => a
		case Call(op) => op().runStateless(db)
		case Read(label, op) => db.withSession(label) { s => op(s).runWithSession(s) }
		case Write(label, op) => db.withTransaction(label) { tx => op(tx).runWithTransaction(tx) }
		case Cont(fa, f) => fa match {
			case Done(a) => f(a).runStateless(db)
			case Call(op) => op().flatMap(f).runStateless(db)
			case Read(label, op) => db.withSession(label) { s => op(s).flatMap(f).runWithSession(s) }
			case Write(label, op) => db.withTransaction(label) { tx => op(tx).flatMap(f).runWithTransaction(tx) }
			case Cont(fb, g) => fb.flatMap(b => g(b).flatMap(f)).runStateless(db)
		}
	}

	@tailrec final def runWithSession(s: DbSession): A = this match {
		case Done(a) => a
		case Call(op) => op().runWithSession(s)
		case Read(label, op) => op(s).runWithSession(s)
		case Write(label, op) => s.withTransaction { tx => op(tx).runWithTransaction(tx) }
		case Cont(fx, f) => fx match {
			case Done(x) => f(x).runWithSession(s)
			case Call(op) => op().flatMap(f).runWithSession(s)
			case Read(_, op) => op(s).flatMap(f).runWithSession(s)
			case Write(_, op) => s.withTransaction { tx => op(tx).flatMap(f).runWithTransaction(tx) }
			case Cont(fy, g) => fy.flatMap(y => g(y).flatMap(f)).runWithSession(s)
		}
	}

	@tailrec final def runWithTransaction(tx: DbTransaction): A = this match {
		case Done(a) => a
		case Call(op) => op().runWithTransaction(tx)
		case Read(_, op) => op(tx).runWithTransaction(tx)
		case Write(_, op) => op(tx).runWithTransaction(tx)
		case Cont(fx, f) => fx match {
			case Done(x) => f(x).runWithTransaction(tx)
			case Call(op) => op().flatMap(f).runWithTransaction(tx)
			case Read(_, op) => op(tx).flatMap(f).runWithTransaction(tx)
			case Write(_, op) => op(tx).flatMap(f).runWithTransaction(tx)
			case Cont(fy, g) => fy.flatMap(y => g(y).flatMap(f)).runWithTransaction(tx)
		}
	}

	def map[B](f: A => B): LazyQueryOld[B] = Monad[LazyQueryOld].map(this)(f)

	def flatMap[B](f: A => LazyQueryOld[B]): LazyQueryOld[B] = this match {
		case Done(a) => Call(() => f(a))
		case c@Call(_) => Cont(c, f)
		case Read(label, op) => Read(label, s => op(s).flatMap(f))
		case Write(label, op) => Write(label, tx => op(tx).flatMap(f))
		// copying the special sauce from scala.util.control.TailCalls
		case c: Cont[a1, b1] => Cont(c.a, (x: a1) => c.f(x).flatMap(f))
	}
}

object LazyQueryOld {
	def pure[A](a: A): LazyQueryOld[A] = Done(a)
	def defer[A](fa: => LazyQueryOld[A]): LazyQueryOld[A] = Call(() => fa)
	def delay[A](a: => A): LazyQueryOld[A] = Call(() => Done(a))
	def read[A](label: String)(op: DbSession => A): LazyQueryOld[A] = Read(label, s => Done(op(s)))
	def write[A](label: String)(op: DbTransaction => A): LazyQueryOld[A] = Write(label, tx => Done(op(tx)))

	def bind[F[_] : CatsDefer : Applicative](db: DB): LazyQueryOld ~> F = new (LazyQueryOld ~> F) {
		def apply[A](fa: LazyQueryOld[A]): F[A] = CatsDefer[F].defer {
			fa.runStateless(db).pure[F]
		}
	}

	private case class Done[A](value: A) extends LazyQueryOld[A]
	private case class Call[A](rest: () => LazyQueryOld[A]) extends LazyQueryOld[A]
	private case class Read[A](label: String, op: DbSession => LazyQueryOld[A]) extends LazyQueryOld[A]
	private case class Write[A](label: String, op: DbTransaction => LazyQueryOld[A]) extends LazyQueryOld[A]
	private case class Cont[X, A](a: LazyQueryOld[X], f: X => LazyQueryOld[A]) extends LazyQueryOld[A] {
		override def flatMap[B](g: A => LazyQueryOld[B]) = Cont[X, B](a, f(_).flatMap(g))
	}

	implicit val catsMonadForLazyQuery: Monad[LazyQueryOld] = new StackSafeMonad[LazyQueryOld] {
		def pure[A](x: A): LazyQueryOld[A] = Done(x)

		def flatMap[A, B](fa: LazyQueryOld[A])(f: A => LazyQueryOld[B]): LazyQueryOld[B] = fa.flatMap(f)
	}

	implicit val catsDeferForLazyQuery: CatsDefer[LazyQueryOld] = new CatsDefer[LazyQueryOld] {
		def defer[A](fa: => LazyQueryOld[A]): LazyQueryOld[A] = Call(() => fa)
	}
}