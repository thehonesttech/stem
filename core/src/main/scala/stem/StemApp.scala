package stem

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import stem.data.AlgebraCombinators.Combinators
import stem.data.{AlgebraCombinators, Committable, ConsumerId, Tagging}
import stem.journal.{EventJournal, JournalEntry}
import stem.readside.ReadSideProcessing.{KillSwitch, Process, RunningProcess}
import stem.readside.{ReadSideProcessing, ReadSideSettings}
import stem.runtime.akka.RuntimeSettings
import stem.runtime.readside.CommittableJournalQuery
import stem.runtime.readside.JournalStores.{memoryCommittableJournalStore, memoryJournalStoreLayer}
import zio.clock.Clock
import zio.duration.durationInt
import zio.stream.ZStream
import zio.{duration, Has, Managed, Queue, Runtime, Schedule, Tag, Task, ULayer, ZEnv, ZIO, ZLayer}

object StemApp {

  type SIO[State, Event, Reject, Result] = ZIO[Combinators[State, Event, Reject], Reject, Result]

  def liveAlgebra[Reject]: AlgebraCombinators[Nothing, Any, Reject] = new AlgebraCombinators[Nothing, Any, Reject] {
    override def read: Task[Nothing] = throw new RuntimeException("This is a stub")

    override def append(es: Any, other: Any*): Task[Unit] = throw new RuntimeException("This is a stub")

    override def reject[A](r: Reject): REJIO[A] = throw new RuntimeException("This is a stub")
  }

  def stubCombinator[State: Tag, Event: Tag, Reject: Tag]: ULayer[Has[AlgebraCombinators[State, Event, Reject]]] =
    ZLayer.succeed(liveAlgebra[Reject])

  def actorSystemLayer(name: String, confFileName: String = "stem.conf") =
    ZLayer.fromManaged(
      Managed.make(Task(ActorSystem(name, ConfigFactory.load(confFileName))))(sys => Task.fromFuture(_ => sys.terminate()).either)
    )

  def readSideStream[Id: Tag, Event: Tag, Offset: Tag](readSideParams: ReadSideParams[Id, Event])(
    implicit runtime: Runtime[ZEnv]
  ): ZStream[Clock with Has[ReadSideProcessing] with Has[CommittableJournalQuery[Offset, Id, Event]], Throwable, KillSwitch] = {
    // simplify and then improve it
    ZStream.accessStream { layers =>
      val readSideProcessing = layers.get[ReadSideProcessing]
      val journal = layers.get[CommittableJournalQuery[Offset, Id, Event]]
      val sources: Seq[ZStream[Clock, Throwable, Committable[JournalEntry[Offset, Id, Event]]]] = readSideParams.tagging.tags.map { tag =>
        journal.eventsByTag(tag, readSideParams.consumerId)
      }
      // convert into process
      ZStream.fromEffect(buildStreamAndProcesses(sources)).flatMap {
        case (streams, processes) =>
          ZStream.fromEffect(readSideProcessing.start(readSideParams.name, processes.toList)).flatMap { ks =>
            // it starts only when all the streams start, it should dynamically merge (see flattenPar but tests fail with it)
            streams
              .map { stream =>
                // ZStream.fromEffect(
                stream
                  .mapMPar(readSideParams.parallelism) { element =>
                    val journalEntry = element.value
                    val commit = element.commit
                    val key = journalEntry.event.entityKey
                    val event = journalEntry.event.payload
                    readSideParams.logic(key, event).retry(Schedule.fixed(1.second)) <* commit
                  }
              }
              .flattenPar(sources.size)
              .as(ks)
          }
      }
    }
  }

  def readSideSubscription[Id: Tag, Event: Tag, Offset: Tag](
    readsideParams: ReadSideParams[Id, Event]
  )(implicit runtime: Runtime[ZEnv]): ZIO[Clock with Has[ReadSideProcessing] with Has[CommittableJournalQuery[Offset, Id, Event]], Throwable, KillSwitch] =
    readSideStream[Id, Event, Offset](readsideParams).runLast.map(_.getOrElse(KillSwitch(Task.unit)))

  case class ReadSideParams[Id, Event](name: String, consumerId: ConsumerId, tagging: Tagging[Id], parallelism: Int = 30, logic: (Id, Event) => Task[Unit])

  private def buildStreamAndProcesses[Offset: Tag, Event: Tag, Id: Tag](
    sources: Seq[ZStream[Clock, Throwable, Committable[JournalEntry[Offset, Id, Event]]]]
  ) = {
    for {
      queue <- Queue.bounded[ZStream[Clock, Throwable, Committable[JournalEntry[Offset, Id, Event]]]](sources.size)
      processes = sources.map { s =>
        Process {
          for {
            stopped <- zio.Promise.make[Throwable, Unit]
            fiber   <- (queue.offer(s.interruptWhen(stopped)) *> stopped.await).fork
          } yield RunningProcess(fiber.join.unit, stopped.succeed().unit)
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

  object Ops {

    implicit class StubbableSio[-R, State: Tag, Event: Tag, Reject: Tag, Result](
      returnType: ZIO[Combinators[State, Event, Reject], Reject, Result]
    ) {
      def provideCombinator: ZIO[Any, Reject, Result] = {
        returnType.provideLayer(stubCombinator[State, Event, Reject])
      }

      def provideSomeCombinator[R0 <: Has[_]](implicit ev: R0 with Combinators[State, Event, Reject] <:< R) =
        returnType.provideSomeLayer[R0](stubCombinator[State, Event, Reject])
    }

  }
}
