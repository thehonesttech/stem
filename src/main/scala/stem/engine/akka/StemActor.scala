package stem.engine.akka

import java.net.URLDecoder
import java.nio.charset.StandardCharsets

import akka.actor.{Actor, ActorLogging, Props, Stash}
import izumi.reflect.Tag
import scodec.bits.BitVector
import stem.communication.internal.macros.RpcMacro
import stem.engine.{AlgebraCombinators, BaseAlgebraCombinators, Fold, Invocation}
import zio.{Has, Runtime, Task, ZEnv, ZLayer}

object StemActor {
  def props[Key: KeyDecoder: Tag, Algebra, State: Tag, Event: Tag, Reject: Tag](
    eventSourcedBehaviour: EventSourcedBehaviour[Algebra, State, Event, Reject],
    baseAlgebraCombinators: BaseAlgebraCombinators[Key, State, Event, Reject]
  )(implicit runtime: Runtime[ZEnv]): Props =
    Props(new StemActor[Key, Algebra, State, Event, Reject](eventSourcedBehaviour, baseAlgebraCombinators))
}

private class StemActor[Key: KeyDecoder: Tag, Algebra, State: Tag, Event: Tag, Reject: Tag](
  eventsourcedBehavior: EventSourcedBehaviour[Algebra, State, Event, Reject],
  keyedAlgebraCombinators: BaseAlgebraCombinators[Key, State, Event, Reject]
)(implicit runtime: Runtime[ZEnv])
    extends Actor
    with Stash
    with ActorLogging {

  private val keyString: String =
    URLDecoder.decode(self.path.name, StandardCharsets.UTF_8.name())

  private val key: Key = KeyDecoder[Key]
    .decode(keyString)
    .getOrElse {
      val error = s"Failed to decode entity id from [$keyString]"
      log.error(error)
      throw new IllegalArgumentException(error)
    }

  override def receive: Receive = {
    case Start =>
      unstashAll()
      context.become(onActions)
    case _ => stash()
  }

  private def onActions: Receive = {
    case CommandInvocation(bytes) =>
      // use macro to do this
      val keyAndFold: ZLayer[Any, Nothing, Has[Key] with Has[Fold[State, Event]]] = ZLayer.succeed(key) ++ ZLayer.succeed(
          eventsourcedBehavior.eventHandler
        )
      val algebraCombinatorsWithKeyResolved = ZLayer.succeed(new AlgebraCombinators[State, Event, Reject] {
        override def read: Task[State] = keyedAlgebraCombinators.read.provideLayer(keyAndFold)

        override def append(es: Event, other: Event*): Task[Unit] = keyedAlgebraCombinators.append(es, other: _*).provideLayer(keyAndFold)

        override def ignore: Task[Unit] = keyedAlgebraCombinators.ignore

        override def reject[A](r: Reject): REJIO[A] = keyedAlgebraCombinators.reject(r)
      })

      //macro creates a map of functions of path -> Invocation
      val invocation: Invocation[State, Event, Reject] = RpcMacro.server[Algebra, State, Event, Reject](eventsourcedBehavior.algebra)

      // decode error and call

      //TODO move from SIO into a (algebraOperators) => IO ?

      sender() ! runtime
        .unsafeRunToFuture(
          invocation
            .call(bytes)
            .provideLayer(algebraCombinatorsWithKeyResolved)
            .mapError(_ => new Exception("TODO: return the reject type"))
        )
        .map(replyBytes => CommandResult(replyBytes))(context.dispatcher)

  }

  private case object Start

}

sealed trait StemCommand
case class CommandInvocation(bytes: BitVector) extends StemCommand

case class CommandResult(bytes: BitVector)

trait KeyDecoder[A] {
  def apply(key: String): Option[A]

  final def decode(key: String): Option[A] = apply(key)
}

object KeyDecoder {
  def apply[A: KeyDecoder] = implicitly[KeyDecoder[A]]
}

trait KeyEncoder[A] {
  def apply(a: A): String

  final def encode(a: A): String = apply(a)
}

object KeyEncoder {
  def apply[A: KeyEncoder] = implicitly[KeyEncoder[A]]
}

case class EventSourcedBehaviour[Algebra, State, Event, Reject](
  algebra: Algebra,
  eventHandler: Fold[State, Event],
  errorHandler: Throwable => Reject
)
