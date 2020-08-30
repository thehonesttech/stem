package stem.test

import java.time.Instant

import scodec.bits.BitVector
import stem.StemApp
import stem.StemApp.SIO
import stem.data.{AlgebraCombinators, StemProtocol, Tagging, Versioned}
import stem.journal.MemoryEventJournal
import stem.runtime.akka.{CommandResult, EventSourcedBehaviour, KeyAlgebraSender, KeyDecoder, KeyEncoder}
import stem.runtime.{AlgebraCombinatorConfig, BaseAlgebraCombinators, Fold, KeyValueStore, KeyedAlgebraCombinators}
import zio.{Has, IO, Ref, Runtime, Tag, Task, UIO, ULayer, ZEnv, ZIO, ZLayer}

import scala.concurrent.duration._

// macro to have methods that return IO instead of SIO? or implicit class that provide the missing layer
object TestStemRuntime {

  def memoryStemtity[Key: Tag, Algebra, State: Tag, Event: Tag, Reject: Tag](
    tagging: Tagging[Key],
    eventSourcedBehaviour: EventSourcedBehaviour[Algebra, State, Event, Reject]
  )(
    implicit runtime: Runtime[ZEnv],
    protocol: StemProtocol[Algebra, State, Event, Reject]
  ): ZIO[Any, Throwable, Key => Algebra] = {
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
    } yield buildTestStemtity(eventSourcedBehaviour, baseAlgebraConfig)
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
                .map {
                  state =>
                    val combinators = new KeyedAlgebraCombinators[Key, State, Event, Reject](
                      key,
                      state,
                      eventSourcedBehaviour.eventHandler,
                      algebraCombinatorConfig
                    )
                    new AlgebraCombinators[State, Event, Reject] {
                      override def read: Task[State] = combinators.read

                      override def append(es: Event, other: Event*): Task[Unit] = combinators.append(es, other: _*)

                      override def ignore: Task[Unit] = combinators.ignore

                      override def reject[A](r: Reject): REJIO[A] = combinators.reject(r)
                    }
                }
                .flatMap { combinator =>
                  val uioCombinator = UIO.succeed(combinator)
                   uioCombinator <* ZIO.effectTotal{
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

trait StemOps {
  implicit val runtime: zio.Runtime[ZEnv] = zio.Runtime.default

  implicit class RichSIO[State: Tag, Event: Tag, Reject: Tag, Result](returnType: SIO[State, Event, Reject, Result]) {
    // I need the key here

    def toIO: IO[Reject, Result] = {
      returnType.provideLayer(StemApp.liveAlgebraLayer[State, Event, Reject])
    }

    def runSync(implicit runtime: Runtime[ZEnv]): Result = {
      val emptyAlgebra = StemApp.liveAlgebraLayer[State, Event, Reject]
      runtime.unsafeRun(returnType.provideLayer(emptyAlgebra))
    }
  }

}
