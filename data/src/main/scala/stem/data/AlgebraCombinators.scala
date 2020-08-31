package stem.data

import zio._

trait AlgebraCombinators[+State, -Event, Reject] {
  type REJIO[Output] = IO[Reject, Output]

  def read: Task[State]
  def append(es: Event, other: Event*): Task[Unit]
  def ignore: Task[Unit] = Task.unit
  def reject[A](r: Reject): REJIO[A]
}

object AlgebraCombinators {
  type Combinators[State, Event, Reject] = Has[AlgebraCombinators[State, Event, Reject]]
}
