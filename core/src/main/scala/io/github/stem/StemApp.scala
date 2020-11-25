package io.github.stem

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import io.github.stem.data.AlgebraCombinators.Combinators
import io.github.stem.data.{AlgebraCombinators, Committable, ConsumerId, Tagging}
import io.github.stem.journal.{EventJournal, JournalEntry}
import io.github.stem.readside.ReadSideProcessing.{KillSwitch, Process, RunningProcess}
import io.github.stem.readside.{ReadSideProcessing, ReadSideSettings}
import io.github.stem.runtime.akka.RuntimeSettings
import io.github.stem.runtime.readside.CommittableJournalQuery
import io.github.stem.runtime.readside.JournalStores.{memoryCommittableJournalStore, memoryJournalStoreLayer}
import zio.clock.Clock
import zio.duration.durationInt
import zio.stream.ZStream
import zio.{duration, Has, IO, Managed, Queue, Runtime, Schedule, Tag, Task, ULayer, ZEnv, ZIO, ZLayer}

object StemApp {

  type SIO[State, Event, Reject, Result] = ZIO[Combinators[State, Event, Reject], Reject, Result]

  def liveAlgebra[Reject]: AlgebraCombinators[Nothing, Any, Reject] = new AlgebraCombinators[Nothing, Any, Reject] {
    override def read: IO[Reject, Nothing] = throw new RuntimeException("This is a stub")

    override def append(es: Any, other: Any*): IO[Reject, Unit] = throw new RuntimeException("This is a stub")

    override def reject[A](r: Reject): IO[Reject, A] = throw new RuntimeException("This is a stub")
  }

  def clientEmptyCombinator[State: Tag, Event: Tag, Reject: Tag]: ULayer[Has[AlgebraCombinators[State, Event, Reject]]] =
    ZLayer.succeed(liveAlgebra[Reject])

  def actorSystemLayer(name: String, confFileName: String = "stem.conf") =
    ZLayer.fromManaged(
      Managed.make(Task(ActorSystem(name, ConfigFactory.load(confFileName))))(sys => Task.fromFuture(_ => sys.terminate()).either)
    )

  def readSideStream[Id: Tag, Event: Tag, Offset: Tag, Reject](readSideParams: ReadSideParams[Id, Event, Reject], errorHandler: Throwable => Reject)(
    implicit runtime: Runtime[ZEnv]
  ): ZStream[Clock with Has[ReadSideProcessing] with Has[CommittableJournalQuery[Offset, Id, Event]], Reject, KillSwitch] = {
    // simplify and then improve it
    ZStream.accessStream { layers =>
      val readSideProcessing = layers.get[ReadSideProcessing]
      val journal = layers.get[CommittableJournalQuery[Offset, Id, Event]]
      val sources: Seq[ZStream[Clock, Reject, Committable[JournalEntry[Offset, Id, Event]]]] = readSideParams.tagging.tags.map { tag =>
        journal.eventsByTag(tag, readSideParams.consumerId).mapError(errorHandler)
      }
      // convert into process
      ZStream.fromEffect(buildStreamAndProcesses(sources)).flatMap {
        case (streams, processes) =>
          ZStream.fromEffect(readSideProcessing.start(readSideParams.name, processes.toList).mapError(errorHandler)).flatMap { ks =>
            // it starts only when all the streams start, it should dynamically merge (see flattenPar but tests fail with it)
            streams
              .map { stream =>
                stream
                  .mapMPar(readSideParams.parallelism) { element =>
                    val journalEntry = element.value
                    val commit = element.commit
                    val key = journalEntry.event.entityKey
                    val event = journalEntry.event.payload
                    readSideParams.logic(key, event).retry(Schedule.fixed(1.second)) <* commit.mapError(errorHandler)
                  }
              }
              .flattenPar(sources.size)
              .as(ks)
          }
      }
    }
  }

  def readSideSubscription[Id: Tag, Event: Tag, Offset: Tag, Reject](
    readsideParams: ReadSideParams[Id, Event, Reject],
    errorHandler: Throwable => Reject
  )(implicit runtime: Runtime[ZEnv]): ZIO[Clock with Has[ReadSideProcessing] with Has[CommittableJournalQuery[Offset, Id, Event]], Reject, KillSwitch] =
    readSideStream[Id, Event, Offset, Reject](readsideParams, errorHandler).runLast.map(_.getOrElse(KillSwitch(Task.unit)))

  case class ReadSideParams[Id, Event, Reject](
    name: String,
    consumerId: ConsumerId,
    tagging: Tagging[Id],
    parallelism: Int = 30,
    logic: (Id, Event) => IO[Reject, Unit]
  )

  private def buildStreamAndProcesses[Offset: Tag, Event: Tag, Id: Tag, Reject](
    sources: Seq[ZStream[Clock, Reject, Committable[JournalEntry[Offset, Id, Event]]]]
  ) = {
    for {
      queue <- Queue.bounded[ZStream[Clock, Reject, Committable[JournalEntry[Offset, Id, Event]]]](sources.size)
      processes = sources.map { s =>
        Process {
          for {
            stopped <- zio.Promise.make[Reject, Unit]
            fiber   <- (queue.offer(s.interruptWhen(stopped)) *> stopped.await).fork
          } yield RunningProcess(fiber.join.unit.mapError(cause => new RuntimeException("Failure " + cause)), stopped.succeed().unit)
        }
      }
    } yield (ZStream.fromQueue(queue), processes)
  }

  def actorSettings(actorSystemName: String) = {
    val actorSystem = StemApp.actorSystemLayer(actorSystemName)
    val readSideSettings = actorSystem to ZLayer.fromService(ReadSideSettings.default)
    val runtimeSettings = actorSystem to ZLayer.fromService(RuntimeSettings.default)
    actorSystem ++ readSideSettings ++ runtimeSettings
  }

  def stemStores[Key: Tag, Event: Tag](
    readSidePollingInterval: duration.Duration = 100.millis
  ) = {
    val memoryEventJournalStore = memoryJournalStoreLayer[Key, Event](readSidePollingInterval)
    val committableJournalQueryStore = memoryEventJournalStore >>> memoryCommittableJournalStore[Key, Event]
    val eventJournalStore: ZLayer[Any, Nothing, Has[EventJournal[Key, Event]]] = memoryEventJournalStore.map { layer =>
      Has(layer.get.asInstanceOf[EventJournal[Key, Event]])
    }
    committableJournalQueryStore ++ eventJournalStore ++ memoryEventJournalStore
  }
  def liveRuntime[Key: Tag, Event: Tag] = {
    (ZEnv.live ++ stemStores[Key, Event]()) ++ ReadSideProcessing.live
  }

  /*def liveRuntime[Key: Tag, Event: Tag](actorSystemName: String) = {
    (ZEnv.live ++ stemStores[Key, Event]() ++ StemApp.actorSettings(actorSystemName)) >+> ReadSideProcessing.live
  } */

  object Ops {

    implicit class StubbableSio[-R, State: Tag, Event: Tag, Reject: Tag, Result](
      returnType: ZIO[Combinators[State, Event, Reject], Reject, Result]
    ) {
      def provideCombinator: ZIO[Any, Reject, Result] = {
        returnType.provideLayer(clientEmptyCombinator[State, Event, Reject])
      }

      def provideSomeCombinator[R0 <: Has[_]](implicit ev: R0 with Combinators[State, Event, Reject] <:< R) =
        returnType.provideSomeLayer[R0](clientEmptyCombinator[State, Event, Reject])
    }

  }
}
