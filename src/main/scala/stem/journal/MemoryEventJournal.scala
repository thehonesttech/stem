package stem.journal
import stem.engine.readside.JournalQuery
import stem.tagging.EventTag
import zio._
import zio.clock.Clock
import zio.duration.Duration
import zio.stream.ZStream

import scala.concurrent.duration.FiniteDuration

class MemoryEventJournal[Key, Event](
  pollingInterval: FiniteDuration,
  internal: Ref[Map[Key, Chunk[(Long, Event, List[String])]]]
) extends EventJournal[Key, Event]
    with JournalQuery[Long, Key, Event] {
  override def append(key: Key, offset: Long, events: NonEmptyChunk[Event]): RIO[HasTagging, Unit] =
    ZIO.accessM { tagging =>
      internal.update { state: Map[Key, Chunk[(Long, Event, List[String])]] =>
        val tags = tagging.tag(key).map(_.value).toList
        val oldValue: Chunk[(Long, Event, List[String])] = state.getOrElse(key, Chunk.empty)
        val newValue: (Key, Chunk[(Long, Event, List[String])]) = key -> (oldValue ++ events.zipWithIndex.map {
            case (event, index) => (index + offset, event, tags)
          })
        state + newValue
      }
    }

  override def read(key: Key, offset: Long): stream.Stream[Nothing, EntityEvent[Key, Event]] = {
    val a: UIO[List[EntityEvent[Key, Event]]] = internal
      .map(_.getOrElse(key, Chunk.empty).toList.drop(offset.toInt).map {
        case (index, event, _) => EntityEvent(key, index, event)
      })
      .get
    stream.Stream.fromIterableM(a)
  }

  override def eventsByTag(tag: EventTag, offset: Option[Long]): ZStream[Clock, Nothing, (Long, EntityEvent[Key, Event])] = {
    stream.Stream.fromSchedule(Schedule.spaced(Duration.fromNanos(pollingInterval.toNanos))) *> currentEventsByTag(tag, offset)
  }

  override def currentEventsByTag(tag: EventTag, offset: Option[Long]): stream.Stream[Nothing, (Long, EntityEvent[Key, Event])] = {
    val a = internal.get.map { state =>
      state
        .flatMap {
          case (key, chunk) =>
            chunk.map {
              case (offset, event, tags) => (key, offset, event, tags)
            }
        }
        .toList
        .sortBy(_._2)
        .drop(offset.getOrElse(0L).toInt)
        .collect {
          case (key, offset, event, tagList) if tagList.contains(tag.value) => offset -> EntityEvent(key, offset, event)
        }
    }

    stream.Stream.fromIterableM(a)
  }
}

object MemoryEventJournal {
  def make[Key, Event](pollingInterval: FiniteDuration): Task[MemoryEventJournal[Key, Event]] = {
    Ref
      .make(Map.empty[Key, Chunk[(Long, Event, List[String])]])
      .map(internal => new MemoryEventJournal[Key, Event](pollingInterval, internal))
  }
}
