package stem.test

import scodec.bits.BitVector
import stem.StemApp
import stem.StemApp.SIO
import stem.data.{AlgebraCombinators, StemProtocol}
import stem.runtime.akka.{CommandResult, KeyAlgebraSender}
import stem.runtime.{BaseAlgebraCombinators, Fold}
import zio.{Has, Runtime, Tag, Task, ULayer, ZEnv, ZLayer}

// macro to have methods that return IO instead of SIO? or implicit class that provide the missing layer
object TestStemRuntime {

  // reuse protocol generation

  def testStemtity[Algebra, Key: Tag, Event: Tag, State: Tag, Reject: Tag](
    commandHandler: Algebra,
    eventHandler: Fold[State, Event],
    combinators: BaseAlgebraCombinators[Key, State, Event, Reject] = ??? //default combinator that tracks events and states
  )(implicit protocol: StemProtocol[Algebra, State, Event, Reject]) = {
    val errorHandler: Throwable => Reject = ???

    KeyAlgebraSender.keyToAlgebra[Key, Algebra, State, Event, Reject](
      { (key: Key, bytes: BitVector) =>
        val keyAndFold: ZLayer[Any, Nothing, Has[Key] with Has[Fold[State, Event]]] = ZLayer.succeed(key) ++ ZLayer.succeed(
          eventHandler
        )
        val algebraCombinatorsWithKeyResolved: ULayer[Has[AlgebraCombinators[State, Event, Reject]]] =
          ZLayer.succeed(new AlgebraCombinators[State, Event, Reject] {
            override def read: Task[State] = combinators.read.provideLayer(keyAndFold)

            override def append(es: Event, other: Event*): Task[Unit] = combinators.append(es, other: _*).provideLayer(keyAndFold)

            override def ignore: Task[Unit] = combinators.ignore

            override def reject[A](r: Reject): REJIO[A] = combinators.reject(r)
          })
        val invocation = protocol.server(commandHandler, errorHandler)
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

    def runSync(implicit runtime: Runtime[ZEnv]): Result = {
      val emptyAlgebra = StemApp.liveAlgebraLayer[State, Event, Reject]
      runtime.unsafeRun(returnType.provideLayer(emptyAlgebra))
    }
  }

}
