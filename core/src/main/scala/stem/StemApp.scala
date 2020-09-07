package stem

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import stem.data.AlgebraCombinators.Combinators
import stem.data.{AlgebraCombinators, Committable, ConsumerId, StemProtocol, Tagging}
import stem.journal.{EventJournal, JournalEntry}
import stem.readside.{ReadSideProcessing, ReadSideSettings}
import stem.readside.ReadSideProcessing.{KillSwitch, Process, RunningProcess}
import stem.runtime.akka.{EventSourcedBehaviour, RuntimeSettings}
import stem.runtime.readside.CommittableJournalQuery
import stem.runtime.readside.JournalStores.{memoryCommittableJournalStore, memoryJournalStoreLayer}
import stem.test.StemtityProbe
import stem.test.TestStemRuntime.stemtity
import zio.clock.Clock
import zio.duration.durationInt
import zio.stream.ZStream
import zio.test.environment.{TestClock, TestConsole}
import zio.{duration, Has, Managed, Queue, Runtime, Schedule, Tag, Task, ULayer, ZEnv, ZIO, ZLayer}

// idempotency, traceId, deterministic tests, schemas in git, restart if unhandled error

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

  def readSide[Id: Tag, Event: Tag, Offset: Tag](
    name: String,
    consumerId: ConsumerId,
    tagging: Tagging[Id],
    parallelism: Int = 30,
    logic: (Id, Event) => Task[Unit]
  )(implicit runtime: Runtime[ZEnv]): ZIO[Clock with Has[ReadSideProcessing] with Has[CommittableJournalQuery[Offset, Id, Event]], Throwable, KillSwitch] = {
    // simplify and then improve it
    ZIO.accessM { layers =>
      val readSideProcessing = layers.get[ReadSideProcessing]
      val journal = layers.get[CommittableJournalQuery[Offset, Id, Event]]
      val sources: Seq[ZStream[Clock, Throwable, Committable[JournalEntry[Offset, Id, Event]]]] = tagging.tags.map { tag =>
        journal.eventsByTag(tag, consumerId)
      }
      // convert into process
      buildStreamAndProcesses(sources).flatMap {
        case (streams, processes) =>
          readSideProcessing.start(name, processes.toList).flatMap { ks =>
            // it starts only when all the streams start, it should dynamically merge (see flattenPar but tests fail with it)
            streams.take(processes.size).runCollect.flatMap { elements =>
              val listOfStreams = elements.toList
              val processingStreams = listOfStreams.map { stream =>
                ZStream.fromEffect(
                  stream
                    .mapMPar(parallelism) { element =>
                      val journalEntry = element.value
                      val commit = element.commit
                      val key = journalEntry.event.entityKey
                      val event = journalEntry.event.payload
                      logic(key, event) <* commit
                    }
                    .runDrain
                    .retry(Schedule.fixed(1.second))
                )
              }
              ZStream.mergeAll(sources.size)(processingStreams: _*).runDrain.fork.as(ks)
            }
          }
      }

    }

  }

  def buildStreamAndProcesses[Offset: Tag, Event: Tag, Id: Tag](
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

  def stemStores[Key: Tag, Event: Tag](
    readSidePollingInterval: duration.Duration = 100.millis
  ) = {
//    val actorSystem = StemApp.actorSystemLayer(actorSystemName)
//    val readSideSettings = actorSystem to ZLayer.fromService(ReadSideSettings.default)
//    val runtimeSettings = actorSystem to ZLayer.fromService(RuntimeSettings.default)
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
