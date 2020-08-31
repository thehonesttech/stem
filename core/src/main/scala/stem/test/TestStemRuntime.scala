package stem.test

import scodec.bits.BitVector
import stem.StemApp
import stem.data.AlgebraCombinators.Combinators
import stem.data._
import stem.journal.MemoryEventJournal
import stem.runtime.akka.{CommandResult, EventSourcedBehaviour, KeyAlgebraSender}
import stem.runtime.{AlgebraCombinatorConfig, KeyValueStore, KeyedAlgebraCombinators}
import zio.clock.Clock
import zio.stream.ZStream
import zio.{Chunk, Has, RIO, Ref, Runtime, Tag, Task, UIO, ULayer, ZEnv, ZIO, ZLayer}

import scala.concurrent.duration._

object TestStemRuntime {

  case class LedgerWithProbe[Key, Algebra, State, Event](algebra: Key => Algebra, probe: Key => Probe[State, Event])

  def memoryStemtity[Key: Tag, Algebra, State: Tag, Event: Tag, Reject: Tag](
    tagging: Tagging[Key],
    eventSourcedBehaviour: EventSourcedBehaviour[Algebra, State, Event, Reject]
  )(
    implicit runtime: Runtime[ZEnv],
    protocol: StemProtocol[Algebra, State, Event, Reject]
  ): ZIO[Any, Throwable, LedgerWithProbe[Key, Algebra, State, Event]] = {
    for {
      memoryEventJournal            <- MemoryEventJournal.make[Key, Event](1.millis)
      memoryEventJournalOffsetStore <- KeyValueStore.memory[Key, Long]
      snapshotKeyValueStore         <- KeyValueStore.memory[Key, Versioned[State]]
      baseAlgebraConfig = AlgebraCombinatorConfig.memory[Key, State, Event](
        memoryEventJournalOffsetStore,
        tagging,
        memoryEventJournal,
        snapshotKeyValueStore
      )
    } yield
      LedgerWithProbe(
        buildTestStemtity(eventSourcedBehaviour, baseAlgebraConfig), { key: Key =>
          new Probe[State, Event] {
            val state: Task[State] = events.flatMap(list => eventSourcedBehaviour.eventHandler.run(Chunk.fromIterable(list)))

            def events: Task[List[Event]] = memoryEventJournal.getAppendedEvent(key)

            def eventStream: ZStream[Any, Throwable, Event] = memoryEventJournal.getAppendedStream(key)

            def eventsFromReadSide(tag: EventTag): RIO[Clock, List[Event]] =
              memoryEventJournal.currentEventsByTag(tag, None).runCollect.map(_.toList.map(_.event.payload))

            def eventStreamFromReadSide(tag: EventTag): ZStream[Clock, Throwable, Event] =
              memoryEventJournal.eventsByTag(tag, None).map(_.event.payload)
          }
        }
      )
  }

  def buildTestStemtity[Algebra, Key: Tag, Event: Tag, State: Tag, Reject: Tag](
    eventSourcedBehaviour: EventSourcedBehaviour[Algebra, State, Event, Reject],
    algebraCombinatorConfig: AlgebraCombinatorConfig[Key, State, Event] //default combinator that tracks events and states
  )(implicit protocol: StemProtocol[Algebra, State, Event, Reject]): Key => Algebra = {
    val errorHandler: Throwable => Reject = eventSourcedBehaviour.errorHandler
    var combinatorMap: Map[Key, UIO[AlgebraCombinators[State, Event, Reject]]] =
      Map[Key, UIO[AlgebraCombinators[State, Event, Reject]]]()

    KeyAlgebraSender.keyToAlgebra[Key, Algebra, State, Event, Reject](
      { (key: Key, bytes: BitVector) =>
        val algebraCombinators: UIO[AlgebraCombinators[State, Event, Reject]] = (for {
          combinatorRetrieved <- combinatorMap.get(key) match {
            case Some(combinator) =>
              combinator
            case None =>
              Ref
                .make[Option[State]](None)
                .map { state =>
                  new KeyedAlgebraCombinators[Key, State, Event, Reject](
                    key,
                    state,
                    eventSourcedBehaviour.eventHandler,
                    algebraCombinatorConfig
                  )
                }
                .flatMap { combinator =>
                  val uioCombinator = UIO.succeed(combinator)
                  uioCombinator <* ZIO.effectTotal {
                    combinatorMap = combinatorMap + (key -> uioCombinator)
                  }
                }
          }
        } yield (combinatorRetrieved))

        val algebraCombinatorsWithKeyResolved: ULayer[Has[AlgebraCombinators[State, Event, Reject]]] =
          ZLayer.fromEffect(algebraCombinators)
        val invocation = protocol.server(eventSourcedBehaviour.algebra, errorHandler)
        invocation
          .call(bytes)
          .map(CommandResult)
          .provideLayer(algebraCombinatorsWithKeyResolved)
      },
      errorHandler
    )(protocol)

  }
}

trait Probe[State, Event] {

  def state: Task[State]

  def events: Task[List[Event]]

  def eventStream: ZStream[Any, Throwable, Event]

  def eventsFromReadSide(tag: EventTag): RIO[Clock, List[Event]]

  def eventStreamFromReadSide(tag: EventTag): ZStream[Clock, Throwable, Event]
}

trait StemOps {
  implicit val runtime: zio.Runtime[ZEnv] = zio.Runtime.default

//  val emptyAlgebra = StemApp.liveAlgebraLayer[State, Event, Reject]
  def testLayer[State: Tag, Event: Tag, Reject: Tag]
    : ZLayer[Any, Nothing, _root_.zio.test.environment.TestEnvironment with Has[AlgebraCombinators[State, Event, Reject]]] =
    zio.test.environment.testEnvironment ++ StemApp.liveAlgebraLayer[State, Event, Reject]

  implicit def autoProvideLiveCombinator[State: Tag, Event: Tag, Reject: Tag, Result](
    zio: ZIO[ZEnv with Combinators[State, Event, Reject], Reject, Result]
  ): ZIO[ZEnv, Reject, Result] =
    zio.provideCustomLayer(StemApp.liveAlgebraLayer[State, Event, Reject])

  //  implicit def toGenericCombinators[State, Event, Reject: Tag](comb: Combinators[State, Event, Reject]): Combinators[Nothing, Any, Reject] =
//    Has(StemApp.liveAlgebra[Reject])

//  implicit class RichSIO[-R, State: Tag, Event: Tag, Reject: Tag, Result](returnType: ZIO[R, Reject, Result])(
//    implicit ev1: Combinators[State, Event, Reject] with TestClock <:< R
//  ) {
//    // I need the key here
//
//    def toIO: IO[Reject, Result] = {
//      returnType.provideLayer(testLayer[State, Event, Reject])
//    }
//
//    def runSync(implicit runtime: Runtime[ZEnv]): Result = {
//      runtime.unsafeRun(returnType.provideLayer(testLayer[State, Event, Reject]))
//    }
//  }

  implicit class RichZIO[Reject, Result](returnType: ZIO[Any, Reject, Result]) {
    // I need the key here

    def runSync(implicit runtime: Runtime[ZEnv]): Result = {
      runtime.unsafeRun(returnType)
    }
  }


  implicit class RichUnsafeZIO[Reject: Tag, Result](returnType: ZIO[Nothing, Reject, Result]) {
    def unsafeRunSync[State: Tag, Event: Tag](implicit runtime: Runtime[ZEnv]): Result = {
      runtime.unsafeRun(returnType.asInstanceOf[ZIO[ZEnv with Combinators[State, Event, Reject], Reject, Result]].provideLayer(testLayer[State,Event,Reject]))
    }
  }
}
