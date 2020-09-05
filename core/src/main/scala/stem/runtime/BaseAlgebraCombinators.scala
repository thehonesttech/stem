package stem.runtime

import izumi.reflect.Tag
import stem.data.{AlgebraCombinators, Tagging, Versioned}
import stem.journal.{EventJournal, MemoryEventJournal}
import stem.runtime.readside.JournalQuery
import stem.snapshot.{KeyValueStore, MemoryKeyValueStore, Snapshotting}
import zio.{Ref, _}


case class AlgebraCombinatorConfig[Key: Tag, State: Tag, Event: Tag](
  eventJournalOffsetStore: KeyValueStore[Key, Long],
  tagging: Tagging[Key],
  eventJournal: EventJournal[Key, Event],
  snapshotting: Snapshotting[Key, State]
)

object AlgebraCombinatorConfig {

  def live[Key: Tag, State: Tag, Event: Tag] =
    ZLayer.fromServices {
      (
        eventJournalOffsetStore: KeyValueStore[Key, Long],
        tagging: Tagging[Key],
        eventJournal: EventJournal[Key, Event],
        snapshotting: Snapshotting[Key, State]
      ) =>
        AlgebraCombinatorConfig(eventJournalOffsetStore, tagging, eventJournal, snapshotting)
    }

  def memory[Key: Tag, State: Tag, Event: Tag](
    memoryEventJournalOffsetStore: KeyValueStore[Key, Long],
    tagging: Tagging[Key],
    memoryEventJournal: EventJournal[Key, Event],
    snapshotKeyValueStore: KeyValueStore[Key, Versioned[State]],
  ): AlgebraCombinatorConfig[Key, State, Event] = {
    new AlgebraCombinatorConfig(
      memoryEventJournalOffsetStore,
      tagging,
      memoryEventJournal,
      Snapshotting.eachVersion(10, snapshotKeyValueStore)
    )
  }

}

// TODO make this a module
class KeyedAlgebraCombinators[Key: Tag, State: Tag, Event: Tag, Reject](
  key: Key,
  state: Ref[Option[State]],
  userBehaviour: Fold[State, Event],
  algebraCombinatorConfig: AlgebraCombinatorConfig[Key, State, Event]
) extends AlgebraCombinators[State, Event, Reject] {
  import algebraCombinatorConfig._
  type Offset = Long

  override def read: Task[State] = {
    val result = state.get.flatMap {
      case Some(state) =>
        IO.succeed(state)
      case None =>
        // read from database (I need the key) and run the events until that state to now
        for {
          stateReturned <- recover
          _             <- state.set(Some(stateReturned))
        } yield stateReturned
    }
    result
  }

  override def append(es: Event, other: Event*): Task[Unit] = {
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

  override def reject[A](r: Reject): IO[Reject, A] = IO.fail(r)

  private def getOffset: Task[Offset] = eventJournalOffsetStore.getValue(key).map(_.getOrElse(0L))

  private def recover: Task[State] = {
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

object EventJournalStore {

  def memory[Key: Tag, Event: Tag]: ZIO[Any, Nothing, MemoryEventJournal[Key, Event]] = {
    import zio.duration._
    for {
      internal <- Ref.make(Chunk[(Key, Long, Event, List[String])]())
      queue <- Queue.unbounded[(Key, Event)]
    } yield new MemoryEventJournal[Key, Event](100.millis, internal, queue)
  }

}

object KeyValueStore {
  def memory[Key: Tag, Value: Tag]: ZIO[Any, Nothing, KeyValueStore[Key, Value]] = {
    for {
      internalKeyValueStore <- Ref.make[Map[Key, Value]](Map.empty)
    } yield new MemoryKeyValueStore[Key, Value](internalKeyValueStore)
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
