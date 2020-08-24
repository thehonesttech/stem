package stem.journal

case class JournalEntry[O, K, E](offset: O, event: EntityEvent[K, E])
