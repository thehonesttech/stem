package stem.test

import scodec.bits.BitVector
import stem.StemApp
import stem.data.AlgebraCombinators.Combinators
import stem.data._
import stem.journal.MemoryEventJournal
import stem.readside.ReadSideProcessing
import stem.runtime.akka.{CommandResult, EventSourcedBehaviour, KeyAlgebraSender}
import stem.runtime.readside.JournalStores.{memoryCommittableJournalStore, memoryJournalStoreLayer}
import stem.runtime.{AlgebraCombinatorConfig, Fold, KeyValueStore, KeyedAlgebraCombinators}
import zio.clock.Clock
import zio.duration.durationInt
import zio.stream.ZStream
import zio.test.environment.{TestClock, TestConsole}
import zio.{duration, Chunk, Has, RIO, Runtime, Tag, Task, UIO, ZEnv, ZIO, ZLayer}

object TestStemRuntime {

  def stemtity[Key: Tag, Algebra, State: Tag, Event: Tag, Reject: Tag](
    tagging: Tagging[Key],
    eventSourcedBehaviour: EventSourcedBehaviour[Algebra, State, Event, Reject]
  )(
    implicit protocol: StemProtocol[Algebra, State, Event, Reject]
  ): ZIO[Has[MemoryEventJournal[Key, Event]], Throwable, Key => Algebra] = ZIO.accessM { layer =>
    val memoryEventJournal = layer.get
    for {
      memoryEventJournalOffsetStore <- KeyValueStore.memory[Key, Long]
      snapshotKeyValueStore         <- KeyValueStore.memory[Key, Versioned[State]]
      baseAlgebraConfig = AlgebraCombinatorConfig.memory[Key, State, Event](
        memoryEventJournalOffsetStore,
        tagging,
        memoryEventJournal,
        snapshotKeyValueStore
      )
    } yield buildTestStemtity(eventSourcedBehaviour, baseAlgebraConfig)
  }

  def stemtityAndReadSideLayer[Key: Tag, Algebra: Tag, State: Tag, Event: Tag, Reject: Tag](
    tagging: Tagging[Key],
    eventSourcedBehaviour: EventSourcedBehaviour[Algebra, State, Event, Reject],
    readSidePollingInterval: duration.Duration = 100.millis
  )(
    implicit protocol: StemProtocol[Algebra, State, Event, Reject]
  ) = {
    val memoryEventJournalLayer = memoryJournalStoreLayer[Key, Event](readSidePollingInterval)
    val stemtityAndProbe = memoryEventJournalLayer >+> (StemtityProbe.live[Key, State, Event](eventSourcedBehaviour.eventHandler) ++ stemtity[
        Key,
        Algebra,
        State,
        Event,
        Reject
      ](
        tagging,
        eventSourcedBehaviour
      ).toLayer ++ memoryCommittableJournalStore[
        Key,
        Event
      ])
    zio.test.environment.TestEnvironment.live ++ TestConsole.any ++ TestClock.any ++ StemApp
      .stubCombinator[State, Event, Reject] ++ stemtityAndProbe ++ ReadSideProcessing.memory
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
              KeyedAlgebraCombinators
                .fromParams[Key, State, Event, Reject](key, eventSourcedBehaviour.eventHandler, algebraCombinatorConfig)
                .flatMap { combinator =>
                  val uioCombinator = UIO.succeed(combinator)
                  uioCombinator <* ZIO.effectTotal {
                    combinatorMap = combinatorMap + (key -> uioCombinator)
                  }
                }
          }
        } yield (combinatorRetrieved))

        protocol
          .server(eventSourcedBehaviour.algebra, errorHandler)
          .call(bytes)
          .map(CommandResult)
          .provideLayer(algebraCombinators.toLayer)
      },
      errorHandler
    )(protocol)

  }
}

object StemtityProbe {

  type StemtityProbe[Key, State, Event] = Has[StemtityProbe.Service[Key, State, Event]]

  case class KeyedProbeOperations[State, Event](
    state: Task[State],
    events: Task[List[Event]],
    eventStream: ZStream[Any, Throwable, Event]
  )

  trait Service[Key, State, Event] {

    def apply(key: Key): KeyedProbeOperations[State, Event]

    def eventsFromReadSide(tag: EventTag): RIO[Clock, List[Event]]

    def eventStreamFromReadSide(tag: EventTag): ZStream[Clock, Throwable, Event]
  }

  def live[Key: Tag, State: Tag, Event: Tag](eventHandler: Fold[State, Event]) = ZLayer.fromService { (memoryEventJournal: MemoryEventJournal[Key, Event]) =>
    new Service[Key, State, Event] {

      def apply(key: Key) = KeyedProbeOperations(
        state = state(key),
        events = events(key),
        eventStream = eventStream(key)
      )
      private val state: Key => Task[State] = key => events(key).flatMap(list => eventHandler.run(Chunk.fromIterable(list)))
      private val events: Key => Task[List[Event]] = key => memoryEventJournal.getAppendedEvent(key)
      private val eventStream: Key => ZStream[Any, Throwable, Event] = key => memoryEventJournal.getAppendedStream(key)

      def eventsFromReadSide(tag: EventTag): RIO[Clock, List[Event]] =
        memoryEventJournal.currentEventsByTag(tag, None).runCollect.map(_.toList.map(_.event.payload))

      def eventStreamFromReadSide(tag: EventTag): ZStream[Clock, Throwable, Event] =
        memoryEventJournal.eventsByTag(tag, None).map(_.event.payload)
    }

  }

}

trait StemOps {
  implicit val runtime: zio.Runtime[ZEnv] = zio.Runtime.default

}

object ZIOOps {

  implicit class ZLayerRich[-RIn: Tag, +E: Tag, +InnerROut: Tag](inner: ZLayer[RIn, E, Has[InnerROut]]) {
    def as[T: Tag](implicit ev: InnerROut <:< T): ZLayer[RIn, E, Has[T]] = {
      inner.map { layer =>
        Has(layer.get.asInstanceOf[T])
      }
    }
  }

  implicit class RichZIO[Reject, Result](returnType: ZIO[Any, Reject, Result]) {
    // I need the key here

    def runSync(implicit runtime: Runtime[ZEnv]): Result = {
      runtime.unsafeRun(returnType)
    }
  }

  def testLayer[State: Tag, Event: Tag, Reject: Tag]
    : ZLayer[Any, Nothing, _root_.zio.test.environment.TestEnvironment with Has[AlgebraCombinators[State, Event, Reject]]] =
    zio.test.environment.testEnvironment ++ StemApp.stubCombinator[State, Event, Reject]

  implicit class RichUnsafeZIO[R, Rej: Tag, Result](returnType: ZIO[R, Rej, Result]) {
    def runSync[State: Tag, Event: Tag, Reject: Tag](implicit runtime: Runtime[ZEnv], ev1: R <:< Combinators[State, Event, Reject]): Result = {
      runtime.unsafeRun(returnType.asInstanceOf[ZIO[ZEnv with Combinators[State, Event, Reject], Reject, Result]].provideLayer(testLayer[State, Event, Reject]))
    }
  }

}
