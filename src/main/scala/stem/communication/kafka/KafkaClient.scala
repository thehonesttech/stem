package stem.communication.kafka

import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import zio.{ZLayer, _}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.kafka.consumer.{Consumer, ConsumerSettings, Subscription}
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde._

class KafkaClient[K: Tag, V: Tag](
  topic: String,
  consumerSettings: ConsumerSettings,
  producerSettings: ProducerSettings,
  keySerde: Serde[Any, K],
  valueSerde: Serde[Any, V]
) {
  type RecordProducer = Producer[Any, K, V]
  val consumerManaged: ZManaged[Clock with Blocking, Throwable, Consumer.Service] =
    Consumer.make(consumerSettings)

  val consumer: ZLayer[Clock with Blocking, Throwable, Consumer] =
    ZLayer.fromManaged(consumerManaged)

  val producerManaged: ZManaged[Any, Throwable, Producer.Service[Any, K, V]] =
    Producer.make[Any, K, V](producerSettings, keySerde, valueSerde)

  val producer: ZLayer[Blocking, Throwable, RecordProducer] = ZLayer.fromManaged(producerManaged)

  def subscribe(fn: (K, V) => Task[Unit]): ZIO[Clock with Blocking, Throwable, Unit] = {

    Consumer
      .subscribeAnd(Subscription.topics(topic))
      .plainStream(keySerde, valueSerde)
      .mapM { record =>
        val key = record.key
        val value = record.value
        fn(key, value)
      }
      .provideSomeLayer(consumer)
      .runDrain

  }

  def produce(key: K, value: V): ZIO[Blocking, Throwable, RecordMetadata] = {
    Producer.produce[Any, K, V](new ProducerRecord(topic, key, value)).provideSomeLayer(producer)
  }

}
