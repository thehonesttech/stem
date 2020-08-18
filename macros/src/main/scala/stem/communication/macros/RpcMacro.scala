package stem.communication.macros

import boopickle.Default._
import izumi.reflect.WeakTag
import scodec.bits.BitVector
import stem.communication.macros.BoopickleCodec._
import stem.data.Invocation
import zio.{Has, Task, ZIO}

import scala.language.experimental.macros
import scala.reflect.ClassTag

object RpcMacro {

  // TODO implement as macro
  def client[Algebra, Reject](fn: BitVector => Task[BitVector], errorHandler: Throwable => Reject): Algebra =
    macro DeriveMacros.client[Algebra, Reject]

  // TODO implement as macro
  def server[Algebra, State, Event, Reject](algebra: Algebra): Invocation[State, Event, Reject] = ???

}
