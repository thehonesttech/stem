package io.github.stem.snapshot

import zio.{Ref, Task}

class MemoryKeyValueStore[K, V](internalMap: Ref[Map[K, V]]) extends KeyValueStore[K, V] {

  override def setValue(key: K, value: V): Task[Unit] = internalMap.update { element =>
    element + (key -> value)
  }

  override def getValue(key: K): Task[Option[V]] = internalMap.get.map(_.get(key))

  override def deleteValue(key: K): Task[Unit] = internalMap.update(_ - key)
}

object MemoryKeyValueStore {
  def make[K, V]: Task[MemoryKeyValueStore[K, V]] = Ref.make(Map.empty[K, V]).map(new MemoryKeyValueStore[K, V](_))
}
