package stem

import stem.data.AlgebraCombinators
import zio.{Has, IO, Tag, Task, ULayer, ZIO, ZLayer}

// idempotency, traceId, deterministic tests, schemas in git, restart if unhandled error

object StemApp {

  def liveAlgebra[State, Event, Reject] = new AlgebraCombinators[State, Event, Reject] {
    override def read: Task[State] = throw new RuntimeException("This is a stub")

    override def append(es: Event, other: Event*): Task[Unit] = throw new RuntimeException("This is a stub")

    override def reject[A](r: Reject): REJIO[A] = throw new RuntimeException("This is a stub")
  }

  def liveAlgebraLayer[State: Tag, Event: Tag, Reject: Tag]: ULayer[Has[AlgebraCombinators[State, Event, Reject]]] =
    ZLayer.succeed(liveAlgebra[State, Event, Reject])

  implicit def clientCombinators[State, Event, Reject, Result](
    from: ZIO[AlgebraCombinators[State, Event, Reject], Reject, Result]
  ): IO[Reject, Result] =
    from.provide(liveAlgebra)

}
