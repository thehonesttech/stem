package io.github.stem.runtime.akka

import akka.actor.{Actor, ActorLogging, Props, ReceiveTimeout, Stash, Status}
import akka.cluster.sharding.ShardRegion
import io.github.stem.data.{Combinators, Invocation, StemProtocol}
import io.github.stem.runtime.{AlgebraCombinatorConfig, Fold, KeyedAlgebraCombinators}
import izumi.reflect.Tag
import scodec.bits.BitVector
import zio.{Has, Runtime, ULayer}

import java.net.URLDecoder
import java.nio.charset.StandardCharsets

object StemActor {
  def props[Key: KeyDecoder: Tag, Algebra, State: Tag, Event: Tag, Reject: Tag](
    eventSourcedBehaviour: EventSourcedBehaviour[Algebra, State, Event, Reject],
    algebraCombinatorConfig: AlgebraCombinatorConfig[Key, State, Event]
  )(implicit protocol: StemProtocol[Algebra, State, Event, Reject]): Props =
    Props(new StemActor[Key, Algebra, State, Event, Reject](eventSourcedBehaviour, algebraCombinatorConfig))
}

private class StemActor[Key: KeyDecoder: Tag, Algebra, State: Tag, Event: Tag, Reject: Tag](
  eventSourcedBehaviour: EventSourcedBehaviour[Algebra, State, Event, Reject],
  algebraCombinatorConfig: AlgebraCombinatorConfig[Key, State, Event]
)(implicit protocol: StemProtocol[Algebra, State, Event, Reject])
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

  private val algebraCombinatorsWithKeyResolved: ULayer[Has[Combinators[State, Event, Reject]]] =
    KeyedAlgebraCombinators
      .fromParams[Key, State, Event, Reject](key, eventSourcedBehaviour.eventHandler, eventSourcedBehaviour.errorHandler, algebraCombinatorConfig)
      .toLayer

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
        protocol.server(eventSourcedBehaviour.algebra, eventSourcedBehaviour.errorHandler)

      sender() ! Runtime.default
        .unsafeRunToFuture(
          invocation
            .call(bytes)
            .provideLayer(algebraCombinatorsWithKeyResolved)
            .mapError { reject =>
              log.error("Failed to decode invocation", reject)
              sender() ! Status.Failure(reject)
              reject
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
