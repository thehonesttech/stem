package stem

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import stem.data.AlgebraCombinators.Combinators
import stem.data.{AlgebraCombinators, Committable, ConsumerId, Tagging}
import stem.journal.JournalEntry
import stem.readside.ReadSideProcessing
import stem.readside.ReadSideProcessing.{KillSwitch, Process, RunningProcess}
import stem.runtime.readside.CommittableJournalQuery
import zio.clock.Clock
import zio.console.Console
import zio.stream.ZStream
import zio.{Has, IO, Managed, Queue, Runtime, Schedule, Tag, Task, ULayer, ZEnv, ZIO, ZLayer}

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
    logic: (Id, Event) => Task[Unit]
  )(
    implicit runtime: Runtime[ZEnv]
  ): ZIO[Console with Has[ReadSideProcessing] with Has[CommittableJournalQuery[Offset, Id, Event]], Throwable, KillSwitch] = {
    // use logic
    ZIO.accessM { layers =>
      val readSideProcessing = layers.get[ReadSideProcessing]
      val journal = layers.get[CommittableJournalQuery[Offset, Id, Event]]
      val sources: Seq[ZStream[Clock, Throwable, Committable[JournalEntry[Offset, Id, Event]]]] = tagging.tags.map { tag =>
        journal.eventsByTag(tag, consumerId)
      }
      // convert into process
      val interruptibleStreamsAndProcesses: ZIO[
        Any,
        Nothing,
        (ZStream[Any, Nothing, ZStream[Clock, Throwable, Committable[JournalEntry[Offset, Id, Event]]]], Seq[Process])
      ] = for {
        queue <- Queue.bounded[ZStream[Clock, Throwable, Committable[JournalEntry[Offset, Id, Event]]]](sources.size)
        processes = sources.map { s =>
          Process {
            zio.Promise.make[Throwable, Unit].flatMap { stopped =>
              (queue.offer(s.interruptWhen(stopped)) *> stopped.await).fork.map { fiber =>
                RunningProcess(fiber.join.as(), stopped.succeed().as())
              }
            }
          }
        }
      } yield (ZStream.fromQueue(queue), processes)

      interruptibleStreamsAndProcesses.flatMap {
        case (streamOfStreams, processes) =>
          readSideProcessing.start(name, processes.toList).flatMap { ks =>
            import zio.duration._
            ZStream
              .mergeAll(processes.size)(streamOfStreams.map { s =>
                s.mapMPar(30)(_.traverse { committable =>
                    val key = committable.event.entityKey
                    val event = committable.event.payload
                    logic(key, event)
                  })
                  .runDrain
                  .retry(Schedule.spaced(1.seconds))

              })
              .runDrain
              .as(ks)
          }
      }
    }
  }

  object Ops {

//    implicit class StubbableSio[-R <: zio.Has[_], State: Tag, Event: Tag, Reject: Tag, Result](
//      returnType: ZIO[R with Combinators[State, Event, Reject], Reject, Result]
//    ) {
//      def stubbedCombinator: ZIO[R, Reject, Result] = returnType.provideSomeLayer[R](stubCombinator[State, Event, Reject])
//    }

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
