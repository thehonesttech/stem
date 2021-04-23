package io.github.stem.journal

import io.github.stem.data.{EntityEvent, EventTag}
import io.github.stem.journal.MemoryEventJournal.Offset
import io.github.stem.runtime.readside.JournalQuery
import zio._
import zio.clock.Clock
import zio.duration.Duration
import zio.stream.ZStream

class MemoryEventJournal[Key, Event](
  pollingInterval: Duration,
  internalStateEvents: Ref[Chunk[(Key, Offset, Event, List[String])]],
  internalQueue: Queue[(Key, Event)],
  clock: Clock.Service
) extends EventJournal[Key, Event]
    with JournalQuery[Offset, Key, Event] {

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
          chunk.map { case (_, offset, event, tags) =>
            (offset, event, tags)
          }
        }
        .toMap
    }
  }

  override def append(key: Key, offset: Offset, events: NonEmptyChunk[Event]): RIO[HasTagging, Unit] =
    ZIO.accessM { tagging =>
      internalStateEvents.update { internalEvents =>
        val tags = tagging.tag(key).map(_.value).toList
        internalEvents ++ events.zipWithIndex.map { case (event, index) =>
          (key, index + offset, event, tags)
        }
      } *> internalQueue.offerAll(events.map(ev => key -> ev)).unit
    }

  override def read(key: Key, offset: Offset): stream.Stream[Nothing, EntityEvent[Key, Event]] = {
    val a: UIO[List[EntityEvent[Key, Event]]] = internal
      .map(_.getOrElse(key, Chunk.empty).toList.drop(offset.toInt).map { case (index, event, _) =>
        EntityEvent(key, index, event)
      })
      .get
    stream.Stream.fromIterableM(a)
  }

  override def eventsByTag(tag: EventTag, offset: Option[Offset]): ZStream[Any, Throwable, JournalEntry[Offset, Key, Event]] = {
    (for {
      lastOffsetProcessed <- ZStream.fromEffect(Ref.make[Option[Offset]](None))
      _                   <- ZStream.fromSchedule(Schedule.fixed(pollingInterval))
      lastOffset <- ZStream
        .fromEffect(lastOffsetProcessed.get)
      journalEntry <- currentEventsByTag(tag, lastOffset.orElse(offset)).mapM { event =>
        lastOffsetProcessed.set(Some(event.offset)).as(event)
      }
    } yield journalEntry).provideLayer(ZLayer.succeed(clock))
  }

  override def currentEventsByTag(tag: EventTag, offset: Option[Offset]): stream.Stream[Throwable, JournalEntry[Offset, Key, Event]] = {
    val a: ZIO[Any, Nothing, List[JournalEntry[Offset, Key, Event]]] = internal.get.map { state =>
      state
        .flatMap { case (key, chunk) =>
          chunk.map { case (offset, event, tags) =>
            (key, offset, event, tags)
          }
        }
        .toList
        .sortBy(_._2)
        .drop(offset.map(_ + 1).getOrElse(0L).toInt)
        .collect {
          case (key, offset, event, tagList) if tagList.contains(tag.value) =>
            JournalEntry(offset, EntityEvent(key, offset, event))
        }
    }
    stream.Stream.fromIterableM(a)
  }
}

object MemoryEventJournal {
  type Offset = Long

  def make[Key, Event](pollingInterval: Duration): ZIO[Clock, Nothing, MemoryEventJournal[Key, Event]] = {
    for {
      internal <- Ref.make(Chunk[(Key, Long, Event, List[String])]())
      queue    <- Queue.unbounded[(Key, Event)]
      clock    <- ZIO.service[Clock.Service]
    } yield new MemoryEventJournal[Key, Event](pollingInterval, internal, queue, clock)
  }
}
