package stem.data

import scodec.bits.BitVector
import zio.{Has, Task, ZIO}

final case class Versioned[A](version: Long, value: A) {
  def traverse[B](f: A => Task[B]): Task[Versioned[B]] =
    f(value).map(Versioned(version, _))
  def map[B](f: A => B): Versioned[B] = Versioned(version, f(value))
}

trait Invocation[State, Event, Reject] {
  def call(message: BitVector): ZIO[Has[AlgebraCombinators[State, Event, Reject]], Reject, BitVector]
}
