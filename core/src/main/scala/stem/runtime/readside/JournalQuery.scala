package stem.runtime.readside

import stem.journal.EntityEvent
import stem.tagging.EventTag
import zio.clock.Clock
import zio.stream.{Stream, ZStream}

// implementations should commit into offset store
trait JournalQuery[O, K, E] {
  def eventsByTag(tag: EventTag, offset: Option[O]): ZStream[Clock, Nothing, (O, EntityEvent[K, E])]

  def currentEventsByTag(tag: EventTag, offset: Option[O]): Stream[Nothing, (O, EntityEvent[K, E])]

}
