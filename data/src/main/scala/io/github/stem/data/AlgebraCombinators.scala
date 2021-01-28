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

//  def accessCombinator[State: Tag, Event: Tag, Reject: Tag, Result: Tag](fn: Service[State, Event, Reject] => IO[Reject, Result]) =
//    ZIO.accessM[Combinators[State, Event, Reject]] { el =>
//      fn(el.get)
//    }

  def read[State: Tag, Event: Tag, Reject: Tag]: ZIO[Combinators[State, Event, Reject], Reject, State] = ZIO.accessM[Combinators[State, Event, Reject]](_.get.read)

  def append[State: Tag, Event: Tag, Reject: Tag](es: Event, other: Event*): ZIO[Combinators[State, Event, Reject], Reject, Unit] = ZIO.accessM[Combinators[State, Event, Reject]](_.get.append(es, other: _*))

  def ignore[State: Tag, Event: Tag, Reject: Tag]: ZIO[Combinators[State, Event, Reject], Nothing, Unit] = ZIO.accessM[Combinators[State, Event, Reject]](_.get.ignore)

  def reject[State: Tag, Event: Tag, Reject: Tag, A](r: Reject): ZIO[Combinators[State, Event, Reject], Reject, A] = ZIO.accessM[Combinators[State, Event, Reject]](_.get.reject(r))

  case class ImpossibleTransitionException[Key, Event, State](state: State, eventPayload: EntityEvent[Key, Event])
      extends RuntimeException(s"Impossible transition from state $state with event $eventPayload")
}
