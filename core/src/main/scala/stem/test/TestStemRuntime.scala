package stem.test

import java.time.Instant

import scodec.bits.BitVector
import stem.StemApp
import stem.StemApp.SIO
import stem.data.{AlgebraCombinators, StemProtocol, Tagging, Versioned}
import stem.journal.MemoryEventJournal
import stem.runtime.LiveBaseAlgebraCombinators.memory
import stem.runtime.akka.{CommandResult, EventSourcedBehaviour, KeyAlgebraSender, KeyDecoder, KeyEncoder}
import stem.runtime.{BaseAlgebraCombinators, Fold, KeyValueStore}
import zio.{Has, IO, Runtime, Tag, Task, ULayer, ZEnv, ZIO, ZLayer}

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
      combinators                   <- memory[Key, State, Event, Reject](memoryEventJournal, memoryEventJournalOffsetStore, snapshotKeyValueStore, tagging)
    } yield buildTestStemtity(eventSourcedBehaviour, combinators)
  }

  def buildTestStemtity[Algebra, Key: Tag, Event: Tag, State: Tag, Reject: Tag](
    eventSourcedBehaviour: EventSourcedBehaviour[Algebra, State, Event, Reject],
    combinators: BaseAlgebraCombinators[Key, State, Event, Reject] //default combinator that tracks events and states
  )(implicit protocol: StemProtocol[Algebra, State, Event, Reject]): Key => Algebra = {
    val errorHandler: Throwable => Reject = eventSourcedBehaviour.errorHandler

    KeyAlgebraSender.keyToAlgebra[Key, Algebra, State, Event, Reject](
      { (key: Key, bytes: BitVector) =>
        println(s"Calling with key $key")
        val keyAndFold: ZLayer[Any, Nothing, Has[Key] with Has[Fold[State, Event]]] = ZLayer.succeed(key) ++ ZLayer.succeed(
          eventSourcedBehaviour.eventHandler
        )
        val algebraCombinatorsWithKeyResolved: ULayer[Has[AlgebraCombinators[State, Event, Reject]]] =
          ZLayer.succeed(new AlgebraCombinators[State, Event, Reject] {
            println(s"Building with Key $key")
            override def read: Task[State] = combinators.read.provideLayer(keyAndFold)

            override def append(es: Event, other: Event*): Task[Unit] = combinators.append(es, other: _*).provideLayer(keyAndFold)

            override def ignore: Task[Unit] = combinators.ignore

            override def reject[A](r: Reject): REJIO[A] = combinators.reject(r)
          })
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
