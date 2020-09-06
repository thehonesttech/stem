package stem.journal

import java.time.Instant

import stem.data.EventTag
import stem.runtime.readside.JournalQuery
import zio._
import zio.clock.Clock
import zio.stream.ZStream
import zio.duration.Duration

//TODO improve performance since they are not great
class MemoryEventJournal[Key, Event](
  pollingInterval: Duration,
  internalStateEvents: Ref[Chunk[(Key, Long, Event, List[String])]],
  internalQueue: Queue[(Key, Event)]
) extends EventJournal[Key, Event]
    with JournalQuery[Long, Key, Event] {

  def getAppendedEvent(key: Key): Task[List[Event]] = internalStateEvents.get.map { list =>
    list.collect {
      case (innerKey, offset, event, tags) if innerKey == key => event
    }.toList
  }

  def getAppendedStream(key: Key): ZStream[Any, Nothing, Event] = ZStream.fromQueue(internalQueue).collect {
    case (internalKey, event) if internalKey == key => event
  }

  private val internal = {
    internalStateEvents.map { elements =>
      elements
        .groupBy { element =>
          element._1
        }
        .view
        .mapValues { chunk =>
          chunk.map {
            case (_, offset, event, tags) => (offset, event, tags)
          }
        }
        .toMap
    }
  }

  override def append(key: Key, offset: Long, events: NonEmptyChunk[Event]): RIO[HasTagging, Unit] =
    ZIO.accessM { tagging =>
      internalStateEvents.update { internalEvents =>
        val tags = tagging.tag(key).map(_.value).toList
        internalEvents ++ events.zipWithIndex.map {
          case (event, index) => (key, index + offset, event, tags)
        }
      } *> internalQueue.offerAll(events.map(ev => key -> ev)).unit
    }

  override def read(key: Key, offset: Long): stream.Stream[Nothing, EntityEvent[Key, Event]] = {
    val a: UIO[List[EntityEvent[Key, Event]]] = internal
      .map(_.getOrElse(key, Chunk.empty).toList.drop(offset.toInt).map {
        case (index, event, _) => EntityEvent(key, index, event)
      })
      .get
    stream.Stream.fromIterableM(a)
  }

  override def eventsByTag(tag: EventTag, offset: Option[Long]): ZStream[Clock, Throwable, JournalEntry[Long, Key, Event]] = {
    stream.Stream.fromSchedule(Schedule.fixed(pollingInterval)) *> currentEventsByTag(tag, offset)
  }

  override def currentEventsByTag(tag: EventTag, offset: Option[Long]): stream.Stream[Throwable, JournalEntry[Long, Key, Event]] = {
    val a: ZIO[Any, Nothing, List[JournalEntry[Long, Key, Event]]] = internal.get.map { state =>
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
          case (key, offset, event, tagList) if tagList.contains(tag.value) =>
            JournalEntry(offset, EntityEvent(key, offset, event))
        }
    }
    stream.Stream.fromIterableM(a)
  }
}

object MemoryEventJournal {
  def make[Key, Event](pollingInterval: Duration): Task[MemoryEventJournal[Key, Event]] = {
    for {
      internal <- Ref.make(Chunk[(Key, Long, Event, List[String])]())
      queue    <- Queue.unbounded[(Key, Event)]
    } yield new MemoryEventJournal[Key, Event](pollingInterval, internal, queue)
  }
}
