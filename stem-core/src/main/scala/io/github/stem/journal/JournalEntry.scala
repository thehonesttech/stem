package io.github.stem.journal

import io.github.stem.data.EntityEvent

case class JournalEntry[O, K, E](offset: O, event: EntityEvent[K, E])
