package io.github.stem.data

import zio._

object AlgebraCombinators {
  type Combinators[State, Event, Reject] = Has[Service[State, Event, Reject]]

  trait Service[+State, -Event, Reject] {
    def read: IO[Reject, State]
    def append(es: Event, other: Event*): IO[Reject, Unit]
    def ignore: UIO[Unit] = IO.unit
    def reject[A](r: Reject): IO[Reject, A]
  }

  def accessCombinator[State: Tag, Event: Tag, Reject: Tag, Result: Tag](fn: Service[State, Event, Reject] => IO[Reject, Result]) =
    ZIO.accessM[Combinators[State, Event, Reject]] { el =>
      fn(el.get)
    }

  case class ImpossibleTransitionException[Key, Event, State](state: State, eventPayload: EntityEvent[Key, Event])
      extends RuntimeException(s"Impossible transition from state $state with event $eventPayload")
}
