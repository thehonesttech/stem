package stem.data

import scodec.bits.BitVector
import zio.{IO, Task}

trait StemProtocol[Algebra, State, Event, Reject] {

  val client: (BitVector => Task[BitVector], Throwable => Reject) => Algebra

  val server: (Algebra, Throwable => Reject) => Invocation[State, Event, Reject]

}