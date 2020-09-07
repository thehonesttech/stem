package stem

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import stem.data.AlgebraCombinators.Combinators
import stem.data.{AlgebraCombinators, Committable, ConsumerId, EventTag, Tagging}
import stem.journal.JournalEntry
import stem.readside.ReadSideProcessing
import stem.readside.ReadSideProcessing.{KillSwitch, Process, RunningProcess}
import stem.runtime.readside.CommittableJournalQuery
import zio.clock.Clock
import zio.console.Console
import zio.duration.durationInt
import zio.stream.ZStream
import zio.{Has, IO, Managed, Queue, RIO, Runtime, Schedule, Tag, Task, ULayer, ZEnv, ZIO, ZLayer}

import scala.collection.mutable

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
                ZStream.fromEffect(stream.mapMPar(parallelism) { element =>
                  val journalEntry = element.value
                  val commit = element.commit
                  val key = journalEntry.event.entityKey
                  val event = journalEntry.event.payload
                  logic(key, event) <* commit
                }.runDrain.retry(Schedule.fixed(1.second)))
              }
              ZStream.mergeAll(sources.size)(processingStreams: _*).runDrain.fork.as(ks)
            }
          }
      }

    }
    //      readSideProcessing.start(name, )
    // processing should be start by process, process should
//      val processingStreams = sources.map { stream =>
//        stream.mapMPar(parallelism) { element =>
//          val journalEntry = element.value
//          val commit = element.commit
//          val key = journalEntry.event.entityKey
//          val event = journalEntry.event.payload
//          logic(key, event) <* commit /* : RIO[Clock, Unit]*/
//        }
//      }
//      ZStream.mergeAll(sources.size)(processingStreams: _*)
//    }
  }

//  def readSide[ Id: Tag, Event: Tag, Offset: Tag](
//    name: String,
//    consumerId: ConsumerId,
//    tagging: Tagging[Id],
//    parallelism: Int,
//    logic: (Id, Event) => Task[Unit]
//  )(
//    implicit runtime: Runtime[ZEnv]
//  ): ZIO[Clock with Has[ReadSideProcessing] with Has[CommittableJournalQuery[Offset, Id, Event]], Throwable, KillSwitch] = {
//    // simplify and then improve it
//    val killSwitch = KillSwitch(Task.unit)
//    readSideStream[Id, Event, Offset](name, consumerId, tagging, parallelism, logic)
//      .interruptWhen(killSwitch.shutdown)
//      .runDrain
//      .fork
//      .as(killSwitch)
//  }

  //  def readSide[Id: Tag, Event: Tag, Offset: Tag](
  //    name: String,
  //    consumerId: ConsumerId,
  //    tagging: Tagging[Id],
  //    parallelism: Int,
  //    logic: (Id, Event) => Task[Unit]
  //  )(
  //    implicit runtime: Runtime[ZEnv]
  //  ): ZIO[Clock with Has[ReadSideProcessing] with Has[CommittableJournalQuery[Offset, Id, Event]], Throwable, KillSwitch] = {
  //    // simplify and then improve it
  //    val killSwitch = KillSwitch(Task.unit)
  //    readSideStream[Id, Event, Offset](name, consumerId, tagging, parallelism, logic)
  //      .interruptWhen(killSwitch.shutdown)
  //      .runDrain
  //      .fork
  //      .as(killSwitch)
  //  }

  //  def readSide[Id: Tag, Event: Tag, Offset: Tag](
  //    name: String,
  //    consumerId: ConsumerId,
  //    tagging: Tagging[Id],
  //    parallelism: Int = 30,
  //    logic: (Id, Event) => Task[Unit]
  //  )(
  //    implicit runtime: Runtime[ZEnv]
  //  ): ZIO[Clock with Has[ReadSideProcessing] with Has[CommittableJournalQuery[Offset, Id, Event]], Throwable, KillSwitch] = {
  //    // simplify and then improve it
  //    ZIO.accessM { layers =>
  //      val readSideProcessing = layers.get[ReadSideProcessing]
  //      val journal = layers.get[CommittableJournalQuery[Offset, Id, Event]]
  //      val sources: Seq[ZStream[Clock, Throwable, Committable[JournalEntry[Offset, Id, Event]]]] = tagging.tags.map { tag =>
  //        journal.eventsByTag(tag, consumerId)
  //      }
  //      // convert into process
  //      val interruptibleStreamsAndProcesses: ZIO[
  //        Any,
  //        Nothing,
  //        (ZStream[Any, Nothing, ZStream[Clock, Throwable, Committable[JournalEntry[Offset, Id, Event]]]], Seq[Process])
  //      ] = buildStreamAndProcesses(sources)
  //
  //      interruptibleStreamsAndProcesses.flatMap {
  //        case (streamOfStreams, processes) =>
  //          readSideProcessing.start(name, processes.toList).flatMap { ks =>
  //            import zio.duration._
  //            val streamsOfLogic: ZStream[Any with Clock, Throwable, Unit] = streamOfStreams.mapM { s =>
  //              val res: ZIO[Any with Clock, Throwable, Unit] = s
  //                .mapMPar(parallelism) { element =>
  //                  element.process { journalEntry =>
  //                    println(s"Processing element $element")
  //                    val key = journalEntry.event.entityKey
  //                    val event = journalEntry.event.payload
  //                    logic(key, event)
  //                  }
  //                }
  //                .runDrain
  //                .retry(Schedule.spaced(1.seconds))
  //              res
  //            }
  //            ZStream
  //              .mergeAll(processes.size)(streamsOfLogic).runDrain.fork.as(ks)
  //          }
  //      }
  //    }
  //  }
  //
  //  private def buildStreamAndProcesses[Offset: Tag, Event: Tag, Id: Tag](
  //    sources: Seq[ZStream[Clock, Throwable, Committable[JournalEntry[Offset, Id, Event]]]]
  //  ) = {
  //    var internalQueue = scala.collection.mutable.Queue[ZStream[Clock, Throwable, Committable[JournalEntry[Offset, Id, Event]]]]()

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
