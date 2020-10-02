package stem.journal

import stem.data.EntityEvent

case class JournalEntry[O, K, E](offset: O, event: EntityEvent[K, E])
