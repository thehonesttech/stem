package stem.runtime.readside

import stem.data.{Committable, ConsumerId}
import stem.journal.{EntityEvent, JournalEntry}
import stem.snapshot.KeyValueStore
import stem.tagging.EventTag
import zio.clock.Clock
import zio.stream.{Stream, ZStream}

// implementations should commit into offset store
trait JournalQuery[O, K, E] {
  def eventsByTag(tag: EventTag, offset: Option[O]): ZStream[Clock, Nothing, JournalEntry[O,K,E]]

  def currentEventsByTag(tag: EventTag, offset: Option[O]): Stream[Nothing, JournalEntry[O,K,E]]

}


trait CommittableJournalQuery[O, K, E] {
  def eventsByTag(tag: EventTag, consumerId: ConsumerId): ZStream[Clock, Nothing, Committable[JournalEntry[O,K,E]]]

  def currentEventsByTag(tag: EventTag, consumerId: ConsumerId): Stream[Nothing, Committable[JournalEntry[O,K,E]]]

}

class CommittableJournalStore[O, K, E](offsetStore: KeyValueStore[K, O], delegateEventJournal: JournalQuery[O, K, E]) extends CommittableJournalQuery[O, K, E] {
  def eventsByTag(tag: EventTag, consumerId: ConsumerId): ZStream[Clock, Nothing, Committable[JournalEntry[O, K, E]]] = ???

  def currentEventsByTag(tag: EventTag, consumerId: ConsumerId): Stream[Nothing, Committable[JournalEntry[O, K, E]]] = ???
}