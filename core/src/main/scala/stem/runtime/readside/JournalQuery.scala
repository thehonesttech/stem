package stem.runtime.readside

import stem.data._
import stem.journal.{EventJournal, JournalEntry, MemoryEventJournal}
import stem.runtime.{EventJournalStore, KeyValueStore}
import stem.snapshot.KeyValueStore
import zio.clock.Clock
import zio.stream.{Stream, ZStream}
import zio.{stream, Has, Tag, Task, ZLayer}

// implementations should commit into offset store
trait JournalQuery[O, K, E] {
  def eventsByTag(tag: EventTag, offset: Option[O]): ZStream[Clock, Throwable, JournalEntry[O, K, E]]

  def currentEventsByTag(tag: EventTag, offset: Option[O]): Stream[Throwable, JournalEntry[O, K, E]]

}

trait CommittableJournalQuery[O, K, E] {
  def eventsByTag(tag: EventTag, consumerId: ConsumerId): ZStream[Clock, Throwable, Committable[JournalEntry[O, K, E]]]

  def currentEventsByTag(tag: EventTag, consumerId: ConsumerId): Stream[Throwable, Committable[JournalEntry[O, K, E]]]

}

class CommittableJournalStore[O, K, E](offsetStore: KeyValueStore[TagConsumer, O], delegateEventJournal: JournalQuery[O, K, E])
    extends CommittableJournalQuery[O, K, E] {

  private def mkCommittableSource[R](
    tag: EventTag,
    consumerId: ConsumerId,
    inner: Option[O] => ZStream[R, Throwable, JournalEntry[O, K, E]]
  ): ZStream[R, Throwable, Committable[JournalEntry[O, K, E]]] = {
    val tagConsumerId = TagConsumer(tag, consumerId)
    stream.Stream
      .fromEffect {
        offsetStore.getValue(tagConsumerId)
      }
      .flatMap(inner)
      .map(x => Committable(offsetStore.setValue(tagConsumerId, x.offset), x))
  }

  def eventsByTag(tag: EventTag, consumerId: ConsumerId): ZStream[Clock, Throwable, Committable[JournalEntry[O, K, E]]] =
    mkCommittableSource(tag, consumerId, delegateEventJournal.eventsByTag(tag, _))

  def currentEventsByTag(tag: EventTag, consumerId: ConsumerId): Stream[Throwable, Committable[JournalEntry[O, K, E]]] = {
    mkCommittableSource(tag, consumerId, delegateEventJournal.currentEventsByTag(tag, _))
  }
}

object JournalStores {
  def memoryJournalStoreLayer[K: Tag, E: Tag]: ZLayer[Any, Nothing, Has[MemoryEventJournal[K, E]]] = {
    EventJournalStore.memory[K, E].toLayer
  }

  def memoryCommittableJournalStore[K: Tag, E: Tag]: ZLayer[Any with Has[MemoryEventJournal[K, E]], Nothing, Has[CommittableJournalQuery[Long, K, E]]] = {
    ZLayer.fromServiceM { (eventJournalStore: MemoryEventJournal[K, E]) =>
      KeyValueStore.memory[TagConsumer, Long].map { readSideOffsetStore =>
        new CommittableJournalStore[Long, K, E](readSideOffsetStore, eventJournalStore)
      }
    }
  }
}

