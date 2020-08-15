package stem

import stem.engine.AlgebraCombinators
import zio.{IO, Task, ZIO}

// idempotency, traceId, deterministic tests, schemas in git, restart if unhandled error

object StemApp {

  def emptyAlgebra[State, Event, Reject] = new AlgebraCombinators[State, Event, Reject] {
    override def read: Task[State] = throw new RuntimeException("This is a stub")

    override def append(es: Event, other: Event*): Task[Unit] = throw new RuntimeException("This is a stub")

    override def reject[A](r: Reject): REJIO[A] = throw new RuntimeException("This is a stub")
  }

  implicit def clientCombinators[State, Event, Reject, Result](
    from: ZIO[AlgebraCombinators[State, Event, Reject], Reject, Result]
  ): IO[Reject, Result] =
    from.provide(emptyAlgebra)

}
