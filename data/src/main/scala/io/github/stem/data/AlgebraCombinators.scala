package io.github.stem.data

import zio._

trait Combinators[+State, -Event, Reject] {
  def read: IO[Reject, State]
  def append(es: Event, other: Event*): IO[Reject, Unit]
  def ignore: UIO[Unit] = IO.unit
  def reject[A](r: Reject): IO[Reject, A]
}

object AlgebraCombinators {

  def read[State: Tag, Event: Tag, Reject: Tag]: ZIO[Has[Combinators[State, Event, Reject]], Reject, State] =
    ZIO.accessM[Has[Combinators[State, Event, Reject]]](_.get.read)

  def append[State: Tag, Event: Tag, Reject: Tag](es: Event, other: Event*): ZIO[Has[Combinators[State, Event, Reject]], Reject, Unit] =
    ZIO.accessM[Has[Combinators[State, Event, Reject]]](_.get.append(es, other: _*))

  def ignore[State: Tag, Event: Tag, Reject: Tag]: ZIO[Has[Combinators[State, Event, Reject]], Nothing, Unit] =
    ZIO.accessM[Has[Combinators[State, Event, Reject]]](_.get.ignore)

  def reject[State: Tag, Event: Tag, Reject: Tag, A](r: Reject): ZIO[Has[Combinators[State, Event, Reject]], Reject, A] =
    ZIO.accessM[Has[Combinators[State, Event, Reject]]](_.get.reject(r))

  case class ImpossibleTransitionException[Key, Event, State](state: State, eventPayload: EntityEvent[Key, Event])
      extends RuntimeException(s"Impossible transition from state $state with event $eventPayload")
}
