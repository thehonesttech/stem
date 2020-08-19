package stem.communication.macros

import scodec.bits.BitVector
import stem.data.Invocation
import zio.Task

import scala.language.experimental.macros

object RpcMacro {

  // TODO implement as macro
  def client[Algebra, Reject](fn: BitVector => Task[BitVector], errorHandler: Throwable => Reject): Algebra =
    macro DeriveMacros.client[Algebra, Reject]

  // TODO implement as macro
  def server[Algebra, State, Event, Reject](algebra: Algebra): Invocation[State, Event, Reject] = ???

}
