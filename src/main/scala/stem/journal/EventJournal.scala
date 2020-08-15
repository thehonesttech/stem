package stem.journal

import stem.tagging.Tagging
import zio.{NonEmptyChunk, RIO, Task}
import zio.stream.Stream

/**
  * Describes abstract event journal.
  *
  * It is expected that sequence number of the first event is 1.
  *
  * @tparam K - entity key type
  * @tparam E - event type
  */
trait EventJournal[K, E] {
  type HasTagging = Tagging[K]
  def append(key: K, offset: Long, events: NonEmptyChunk[E]): RIO[HasTagging, Unit]
  def read(key: K, offset: Long): Stream[Nothing, EntityEvent[K, E]]
}

final case class EntityEvent[K, A](entityKey: K, sequenceNr: Long, payload: A) {
  def map[B](f: A => B): EntityEvent[K, B] = copy(payload = f(payload))
}
