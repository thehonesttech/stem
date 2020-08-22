package stem

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import stem.data.AlgebraCombinators
import stem.data.AlgebraCombinators.Combinators
import zio.{Has, IO, Managed, Tag, Task, ULayer, ZIO, ZLayer}

// idempotency, traceId, deterministic tests, schemas in git, restart if unhandled error

object StemApp {

  type SIO[State, Event, Reject, Result] = ZIO[Combinators[State, Event, Reject], Reject, Result]

  def liveAlgebra[State, Event, Reject]: AlgebraCombinators[State, Event, Reject] = new AlgebraCombinators[State, Event, Reject] {
    override def read: Task[State] = throw new RuntimeException("This is a stub")

    override def append(es: Event, other: Event*): Task[Unit] = throw new RuntimeException("This is a stub")

    override def reject[A](r: Reject): REJIO[A] = throw new RuntimeException("This is a stub")
  }

  def liveAlgebraLayer[State: Tag, Event: Tag, Reject: Tag]: ULayer[Has[AlgebraCombinators[State, Event, Reject]]] =
    ZLayer.succeed(liveAlgebra[State, Event, Reject])

  def actorSystemLayer(name: String, confFileName: String = "stem.conf") =
    ZLayer.fromManaged(
      Managed.make(Task(ActorSystem(name, ConfigFactory.load(confFileName))))(sys => Task.fromFuture(_ => sys.terminate()).either)
    )

  implicit def clientCombinators[State, Event, Reject, Result](
    from: ZIO[AlgebraCombinators[State, Event, Reject], Reject, Result]
  ): IO[Reject, Result] =
    from.provide(liveAlgebra)

}
