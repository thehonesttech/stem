package stem.runtime.akka

import java.net.URLDecoder
import java.nio.charset.StandardCharsets

import akka.actor.{Actor, ActorLogging, Props, ReceiveTimeout, Stash, Status}
import akka.cluster.sharding.ShardRegion
import izumi.reflect.Tag
import scodec.bits.BitVector
import stem.data.StemProtocol
import stem.data.{AlgebraCombinators, Invocation}
import stem.runtime.{AlgebraCombinatorConfig, BaseAlgebraCombinators, Fold, KeyedAlgebraCombinators}
import zio.{Has, Ref, Runtime, Task, ULayer, ZEnv, ZIO, ZLayer}

object StemActor {
  def props[Key: KeyDecoder: Tag, Algebra, State: Tag, Event: Tag, Reject: Tag](
    eventSourcedBehaviour: EventSourcedBehaviour[Algebra, State, Event, Reject],
    algebraCombinatorConfig: AlgebraCombinatorConfig[Key, State, Event]
  )(implicit runtime: Runtime[ZEnv], protocol: StemProtocol[Algebra, State, Event, Reject]): Props =
    Props(new StemActor[Key, Algebra, State, Event, Reject](eventSourcedBehaviour, algebraCombinatorConfig))
}

private class StemActor[Key: KeyDecoder: Tag, Algebra, State: Tag, Event: Tag, Reject: Tag](
  eventsourcedBehavior: EventSourcedBehaviour[Algebra, State, Event, Reject],
  algebraCombinatorConfig: AlgebraCombinatorConfig[Key, State, Event]
)(implicit runtime: Runtime[ZEnv], protocol: StemProtocol[Algebra, State, Event, Reject])
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

  private val algebraCombinators = Ref
    .make[Option[State]](None)
    .map { state =>
      val combinators = new KeyedAlgebraCombinators[Key, State, Event, Reject](
        key,
        state,
        eventsourcedBehavior.eventHandler,
        algebraCombinatorConfig
      )
      new AlgebraCombinators[State, Event, Reject] {
        override def read: Task[State] = combinators.read

        override def append(es: Event, other: Event*): Task[Unit] = combinators.append(es, other: _*)

        override def ignore: Task[Unit] = combinators.ignore

        override def reject[A](r: Reject): REJIO[A] = combinators.reject(r)
      }
    }

  private val algebraCombinatorsWithKeyResolved: ULayer[Has[AlgebraCombinators[State, Event, Reject]]] = {
    ZLayer.fromEffect(algebraCombinators)
  }

  override def receive: Receive = {
    case Start =>
      unstashAll()
      context.become(onActions)
    case _ => stash()
  }

  // here key is available, so at this level we can store the state of the algebra
  private def onActions: Receive = {
    case CommandInvocation(bytes) =>
      //macro creates a map of functions of path -> Invocation
      val invocation: Invocation[State, Event, Reject] =
        protocol.server(eventsourcedBehavior.algebra, eventsourcedBehavior.errorHandler)

      sender() ! runtime
        .unsafeRunToFuture(
          invocation
            .call(bytes)
            .provideLayer(algebraCombinatorsWithKeyResolved)
            .mapError { reject =>
              val decodingError = new IllegalArgumentException(s"Reject error ${reject}")
              log.error(decodingError, "Failed to decode invocation")
              sender() ! Status.Failure(decodingError)
              decodingError
            }
        )
        .map(replyBytes => CommandResult(replyBytes))(context.dispatcher)

    case ReceiveTimeout =>
      passivate()
    case Stop =>
      context.stop(self)
  }

  private def passivate(): Unit = {
    log.debug("Passivating...")
    context.parent ! ShardRegion.Passivate(Stop)
  }

  private case object Start

}

sealed trait StemCommand
case class CommandInvocation(bytes: BitVector) extends StemCommand

case class CommandResult(bytes: BitVector)

case object Stop

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
