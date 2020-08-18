package stem.runtime

import scodec.bits.BitVector
import stem.data.Invocation
import zio.Task

trait StemProtocol[Algebra, State, Event, Reject] {

  val client: (BitVector => Task[BitVector], Throwable => Reject) => Algebra

  val server: (Algebra, Throwable => Reject) => Invocation[State, Event, Reject]

}
