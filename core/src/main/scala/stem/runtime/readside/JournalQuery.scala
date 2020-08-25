package stem.runtime.readside

import stem.data._
import stem.journal.{EventJournal, JournalEntry}
import stem.runtime.{EventJournalStore, KeyValueStore}
import stem.snapshot.KeyValueStore
import zio.clock.Clock
import zio.stream.{Stream, ZStream}
import zio.{Has, Tag, ZLayer, stream}

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
  def memoryJournalAndQueryStoreLayer[K: Tag, E: Tag]: (ZLayer[Any, Nothing, Has[EventJournal[K, E]]], ZLayer[Any, Nothing, Has[CommittableJournalQuery[Long, K, E]]]) = {
    val res = for {
      memoryStore         <- EventJournalStore.memory[K, E]
      readSideOffsetStore <- KeyValueStore.memory[TagConsumer, Long]
    } yield (memoryStore: EventJournal[K, E], new CommittableJournalStore[Long, K, E](readSideOffsetStore, memoryStore): CommittableJournalQuery[Long, K, E])
    ZLayer.fromEffect(res.map(_._1)) -> ZLayer.fromEffect(res.map(_._2))

  }
}

case class Stores[K, E, S](
  eventJournalStore: EventJournal[K, E],
  committableJournalQueryStore: CommittableJournalQuery[Long, K, E],
  memoryEventJournalOffsetStore: KeyValueStore[K, Long],
  snapshotKeyValueStore: KeyValueStore[K, Versioned[S]],
)
