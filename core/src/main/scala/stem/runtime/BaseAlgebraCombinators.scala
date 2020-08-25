package stem.runtime

import izumi.reflect.Tag
import stem.data.{Tagging, Versioned}
import stem.journal.{EventJournal, MemoryEventJournal}
import stem.runtime.readside.JournalQuery
import stem.snapshot.{KeyValueStore, MemoryKeyValueStore, Snapshotting}
import zio.{Ref, _}

/**
  * Algebra that depends on key (you can override this)
  *
  * @tparam Key
  * @tparam State
  * @tparam Event
  * @tparam Reject
  */
trait BaseAlgebraCombinators[Key, State, Event, Reject] {
  type KeyAndFold = Has[Key] with Has[Fold[State, Event]]

  def read: RIO[KeyAndFold, State]

  def append(es: Event, other: Event*): RIO[KeyAndFold, Unit]

  def ignore: Task[Unit] = Task.unit

  def reject(r: Reject): IO[Reject, Nothing]
}

class LiveBaseAlgebraCombinators[Key: Tag, State: Tag, Event: Tag, Reject](
  // TODO use a better type for the tag
  //  tag: String,
  state: Ref[Option[State]],
  eventJournalOffsetStore: KeyValueStore[Key, Long],
  tagging: Tagging[Key],
  eventJournal: EventJournal[Key, Event],
  snapshotting: Snapshotting[Key, State]
) extends BaseAlgebraCombinators[Key, State, Event, Reject] {

  type Offset = Long

  override def read: RIO[KeyAndFold, State] = state.get.flatMap {
    case Some(state) =>
      IO.succeed(state)
    case None =>
      // read from database (I need the key) and run the events until that state to now
      for {
        stateReturned <- recover
        _             <- state.set(Some(stateReturned))
      } yield stateReturned
  }

  override def append(es: Event, other: Event*): RIO[KeyAndFold, Unit] = {
    ZIO.accessM { layer =>
      val key = layer.get[Key]
      val userBehaviour = layer.get[Fold[State, Event]]
      //append event and store offset
      // read the offset by key
      for {
        offset <- getOffset
        events: NonEmptyChunk[Event] = NonEmptyChunk(es, other: _*)
        currentState <- read
        newState     <- userBehaviour.init(currentState).run(events)
        _            <- state.set(Some(newState))
        _            <- eventJournal.append(key, offset, events).provide(tagging)
        _            <- snapshotting.snapshot(key, Versioned(offset, currentState), Versioned(offset + events.size, newState))
        _            <- eventJournalOffsetStore.setValue(key, offset + events.size)
      } yield ()
    }
  }

  override def reject(r: Reject): IO[Reject, Nothing] = IO.fail(r)

  private def getOffset: RIO[Has[Key], Offset] = ZIO.accessM { layer =>
    eventJournalOffsetStore.getValue(layer.get[Key]).map(_.getOrElse(0L))
  }

  private def recover: RIO[KeyAndFold, State] = {
    ZIO.accessM { layer =>
      val key = layer.get[Key]
      val userBehaviour = layer.get[Fold[State, Event]]
      snapshotting.load(key).flatMap { versionedStateMaybe =>
        // if nothing there, get initial state
        // I need current offset from offset store
        val (offset, readStateFromSnapshot) =
          versionedStateMaybe.fold(0L -> userBehaviour.initial)(versionedState => versionedState.version -> versionedState.value)

        // read until the current offset
        getOffset.flatMap {
          case offsetValue if offsetValue > 0 =>
            // read until offsetValue
            val foldBehaviour = userBehaviour.init(readStateFromSnapshot)
            eventJournal
              .read(key, offset)
              .foldWhileM(readStateFromSnapshot -> offset) { case (_, foldedOffset) => foldedOffset == offsetValue } {
                case ((state, _), entityEvent) =>
                  foldBehaviour
                    .reduce(state, entityEvent.payload)
                    .map { processedState =>
                      processedState -> entityEvent.sequenceNr
                    }
              }
              .map(_._1)

          case _ => Task.succeed(readStateFromSnapshot)

        }

      }
    }
  }

}

object EventJournalStore {

  def memory[Key: Tag, Event: Tag]: ZIO[Any, Nothing, EventJournal[Key, Event] with JournalQuery[Long, Key, Event]] = {
    import scala.concurrent.duration._
    for {
      eventJournalInternal <- Ref.make[Map[Key, Chunk[(Long, Event, List[String])]]](Map.empty)
    } yield new MemoryEventJournal[Key, Event](100.millis, eventJournalInternal)
  }

}

object KeyValueStore {
  def memory[Key: Tag, Value: Tag]: ZIO[Any, Nothing, KeyValueStore[Key, Value]] = {
    for {
      internalKeyValueStore <- Ref.make[Map[Key, Value]](Map.empty)
    } yield new MemoryKeyValueStore[Key, Value](internalKeyValueStore)
  }
}

object LiveBaseAlgebraCombinators {
  def memory[Key: Tag, State: Tag, Event: Tag, Reject](
    memoryEventJournal: EventJournal[Key, Event],
    memoryEventJournalOffsetStore: KeyValueStore[Key, Long],
    snapshotKeyValueStore: KeyValueStore[Key, Versioned[State]],
    tagging: Tagging[Key]
  ): ZIO[Any, Nothing, LiveBaseAlgebraCombinators[Key, State, Event, Reject]] = {
    for {
      state <- Ref.make[Option[State]](None)
      snapshotting = Snapshotting.eachVersion(10, snapshotKeyValueStore)
    } yield new LiveBaseAlgebraCombinators(state, memoryEventJournalOffsetStore, tagging, memoryEventJournal, snapshotting)
  }
}

// TODO: can the output be a Task or must it be a State?
final case class Fold[State, Event](initial: State, reduce: (State, Event) => Task[State]) {
  def init(a: State): Fold[State, Event] = copy(initial = a)

  def contramap[Y](f: Y => Event): Fold[State, Y] = Fold(initial, (a, c) => reduce(a, f(c)))

  def run(gb: Chunk[Event]): Task[State] =
    gb.foldM(initial)(reduce)

  def focus[B](get: State => B)(set: (State, B) => State): Fold[B, Event] =
    Fold(get(initial), (s, e) => reduce(set(initial, s), e).map(get))

  def expand[B](init: State => B, read: B => State, update: (B, State) => B): Fold[B, Event] =
    Fold(init(initial), (current, e) => reduce(read(current), e).map(update(current, _)))
}

object Fold {

  def count[A]: Fold[Long, A] =
    Fold(0L, (c, _) => Task.succeed(c + 1L))
}
