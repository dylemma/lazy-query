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
sealed trait LazyQuery[+A] {
	import LazyQuery._

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

	def map[B](f: A => B): LazyQuery[B] = Monad[LazyQuery].map(this)(f)

	def flatMap[B](f: A => LazyQuery[B]): LazyQuery[B] = this match {
		case Done(a) => Call(() => f(a))
		case c@Call(_) => Cont(c, f)
		case Read(label, op) => Read(label, s => op(s).flatMap(f))
		case Write(label, op) => Write(label, tx => op(tx).flatMap(f))
		// copying the special sauce from scala.util.control.TailCalls
		case c: Cont[a1, b1] => Cont(c.a, (x: a1) => c.f(x).flatMap(f))
	}
}

object LazyQuery {
	def pure[A](a: A): LazyQuery[A] = Done(a)
	def defer[A](fa: => LazyQuery[A]): LazyQuery[A] = Call(() => fa)
	def delay[A](a: => A): LazyQuery[A] = Call(() => Done(a))
	def read[A](label: String)(op: DbSession => A): LazyQuery[A] = Read(label, s => Done(op(s)))
	def write[A](label: String)(op: DbTransaction => A): LazyQuery[A] = Write(label, tx => Done(op(tx)))

	def bind[F[_] : CatsDefer : Applicative](db: DB): LazyQuery ~> F = new (LazyQuery ~> F) {
		def apply[A](fa: LazyQuery[A]): F[A] = CatsDefer[F].defer {
			fa.runStateless(db).pure[F]
		}
	}

	private case class Done[A](value: A) extends LazyQuery[A]
	private case class Call[A](rest: () => LazyQuery[A]) extends LazyQuery[A]
	private case class Read[A](label: String, op: DbSession => LazyQuery[A]) extends LazyQuery[A]
	private case class Write[A](label: String, op: DbTransaction => LazyQuery[A]) extends LazyQuery[A]
	private case class Cont[X, A](a: LazyQuery[X], f: X => LazyQuery[A]) extends LazyQuery[A] {
		override def flatMap[B](g: A => LazyQuery[B]) = Cont[X, B](a, f(_).flatMap(g))
	}

	implicit val catsMonadForLazyQuery: Monad[LazyQuery] = new StackSafeMonad[LazyQuery] {
		def pure[A](x: A): LazyQuery[A] = Done(x)

		def flatMap[A, B](fa: LazyQuery[A])(f: A => LazyQuery[B]): LazyQuery[B] = fa.flatMap(f)
	}

	implicit val catsDeferForLazyQuery: CatsDefer[LazyQuery] = new CatsDefer[LazyQuery] {
		def defer[A](fa: => LazyQuery[A]): LazyQuery[A] = Call(() => fa)
	}
}