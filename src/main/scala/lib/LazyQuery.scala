package lib

import cats.syntax.applicative._
import cats.{Applicative, Defer => CatsDefer, MonadError, StackSafeMonad, ~>}

import scala.annotation.tailrec
import scala.util.control.NonFatal

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
	import LazyQuery.*

	def flatMap[B](f: A => LazyQuery[B]): LazyQuery[B] = FlatMap(this, f)
	def map[B](f: A => B): LazyQuery[B] = FlatMap(this, a => Done(f(a)))
	def withSideEffect(f: (A, Option[DbTransaction]) => Unit): LazyQuery[A] = flatMap { a => SideEffect(f(a, _)).map(_ => a) }
	def handleError[B >: A](f: Throwable => B): LazyQuery[B] = MonadError[LazyQuery, Throwable].handleError(this)(f)
	def handleErrorWith[B >: A](f: Throwable => LazyQuery[B]): LazyQuery[B] = MonadError[LazyQuery, Throwable].handleErrorWith(this)(f)
	def transactional(label: String): LazyQuery[A] = WithTransaction(label, _ => this)

	final def runStateless(db: DB): A = advance(this, PendingDb(db))
	final def runWithSession(s: DbSession): A = advance(this, InSession(s))
	final def runWithTransaction(tx: DbTransaction): A = advance(this, InTransaction(tx))
}

object LazyQuery {

	def pure[A](a: A): LazyQuery[A] = Done(a)
	def defer[A](fa: => LazyQuery[A]): LazyQuery[A] = Defer(() => fa)
	def delay[A](a: => A): LazyQuery[A] = Defer(() => Done(a))
	def read[A](label: String)(op: DbSession => A): LazyQuery[A] = WithSession(label, s => Done(op(s)))
	def write[A](label: String)(op: DbTransaction => A): LazyQuery[A] = WithTransaction(label, tx => Done(op(tx)))
	def raiseError(err: Throwable): LazyQuery[Nothing] = Defer(() => throw err)

	def bind[F[_] : CatsDefer : Applicative](db: DB): LazyQuery ~> F = new (LazyQuery ~> F) {
		def apply[A](fa: LazyQuery[A]): F[A] = CatsDefer[F].defer {
			advance(fa, PendingDb(db)).pure[F]
		}
	}

	/* Implementation note:
	 * The Done/Defer/FlatMap classes are essentially a copy of scala.util.control.TailCalls's `Done/Call/Cont` classes, 
	 * with the goal of stack-safety in the `flatMap` and `tailRecM` monad methods.
	 * Read and Write are like Call, but act as a gate that prompts the runner to open
	 * a session or transaction if one isn't currently in use.
	 */
	
	private case class Done[A](result: A) extends LazyQuery[A]
	private case class Defer[A](f: () => LazyQuery[A]) extends LazyQuery[A]
	private case class WithSession[A](label: String, f: DbSession => LazyQuery[A]) extends LazyQuery[A]
	private case class WithTransaction[A](label: String, f: DbTransaction => LazyQuery[A]) extends LazyQuery[A]
	private case class SideEffect(f: Option[DbTransaction] => Unit) extends LazyQuery[Unit]
	private case class Recovery[A](inner: LazyQuery[A], recover: Throwable => LazyQuery[A]) extends LazyQuery[A]
	private case class FlatMap[X, A](prev: LazyQuery[X], next: X => LazyQuery[A]) extends LazyQuery[A]

	implicit val catsMonadForLazyQuery: MonadError[LazyQuery, Throwable] = new MonadError[LazyQuery, Throwable] with StackSafeMonad[LazyQuery] {
		def pure[A](x: A): LazyQuery[A] = Done(x)
		def flatMap[A, B](fa: LazyQuery[A])(f: A => LazyQuery[B]): LazyQuery[B] = fa.flatMap(f)
		def raiseError[A](e: Throwable): LazyQuery[A] = Defer { () => throw e }
		def handleErrorWith[A](fa: LazyQuery[A])(f: Throwable => LazyQuery[A]): LazyQuery[A] = Recovery(fa, f)
	}

	implicit val catsDeferForLazyQuery: CatsDefer[LazyQuery] = new CatsDefer[LazyQuery] {
		def defer[A](fa: => LazyQuery[A]): LazyQuery[A] = Defer(() => fa)
	}

	// states
	private sealed trait RunState
	private case class PendingDb(db: DB) extends RunState {
		def startSession[A](step: WithSession[A]): A =
			db.withSession(step.label) { s => advance(step.f(s), InSession(s)) }
		def startTransaction[A](step: WithTransaction[A]): A =
			db.withTransaction(step.label) { tx => advance(step.f(tx), InTransaction(tx)) }
	}
	private case class InSession(s: DbSession) extends RunState {
		def upgradeToTransaction[A](step: WithTransaction[A]): A =
			s.withTransaction { tx => advance(step.f(tx), InTransaction(tx)) }
	}
	private case class InTransaction(tx: DbTransaction) extends RunState

	@tailrec private def advance[A](step: LazyQuery[A], state: RunState): A = step match {
		case Done(result) => result
		case Defer(f) => advance(f(), state)
		case step@WithSession(label, f) => state match {
			case pending: PendingDb => pending.startSession(step)
			case InSession(s) => advance(f(s), state)
			case InTransaction(tx) => advance(f(tx), state)
		}
		case step@WithTransaction(label, f) => state match {
			case pending: PendingDb => pending.startTransaction(step)
			case inSession: InSession => inSession.upgradeToTransaction(step)
			case InTransaction(tx) => advance(f(tx), state)
		}
		case Recovery(inner, recover) => runRecovery(inner, recover, state)
		case SideEffect(f) => state match {
			case InTransaction(tx) => f(Some(tx))
			case _ => f(None)
		}
		case FlatMap(prev, f) => prev match {
			case Done(x) => advance(f(x), state)
			case Defer(x) => advance(x().flatMap(f), state)
			case WithSession(label, a) => advance(WithSession(label, a(_).flatMap(f)), state)
			case WithTransaction(label, a) => advance(WithTransaction(label, a(_).flatMap(f)), state)
			case Recovery(x, r) => advance(Recovery(x.flatMap(f), r(_).flatMap(f)), state)
			case SideEffect(e) =>
				state match {
					case InTransaction(tx) => e(Some(tx))
					case _ => e(None)
				}
				advance(f(()), state)
			case FlatMap(py, g) => advance(py.flatMap(y => g(y).flatMap(f)), state)
		}
	}

	private def runRecovery[A](inner: LazyQuery[A], recover: Throwable => LazyQuery[A], state: RunState): A = {
		/* If the `inner` LQ would start a new session before throwing an exception,
		 * that session should end, and we resume execution without that session.
		 * Similar, for transactions, where the exception would bubble up through
		 * the transaction, causing it to roll back, and we would have to continue
		 * from the original state instead of an `InTransaction` state.
		 * 
		 * If the `inner` LQ does not start any new session or transaction before throwing,
		 * then the state is unchanged anyway.
		 *
		 * Therefore, there's no need to capture the new state after running `inner`.
		 * We can simply pass the original state to `advance` in all cases.
		 */
		try advance(inner, state)
		catch {case NonFatal(e) => advance(recover(e), state)}
	}
}
