package stem.communication.kafka

import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import zio.Schedule.Decision
import zio.{Tag, ZLayer, _}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.{Console, putStrLn}
import zio.duration.durationInt
import zio.kafka.consumer.{Consumer, ConsumerSettings, Subscription}
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde._

class KafkaConsumer[K: Tag, V: Tag](
  consumerConfiguration: KafkaConsumerConfiguration[K, V]
) {
  type RecordProducer = Producer[Any, K, V]
  val consumerManaged: ZManaged[Clock with Blocking, Throwable, Consumer.Service] =
    Consumer.make(consumerConfiguration.consumerSettings)

  val consumer: ZLayer[Clock with Blocking, Throwable, Consumer] =
    ZLayer.fromManaged(consumerManaged)

  private val scheduleEvery5Seconds = Schedule.spaced(5.seconds).onDecision({
    case Decision.Done(_)                 => putStrLn(s"Reconnection successfull")
    case Decision.Continue(attempt, _, _) => putStrLn(s"Error while reconnecting attempt #$attempt")
  })

  def subscribe[Reject](fn: (K, V) => Task[Unit]): ZIO[Clock with Blocking with Console, Throwable, Unit] = {

    Consumer
      .subscribeAnd(Subscription.topics(consumerConfiguration.topic))
      .plainStream(consumerConfiguration.keySerde, consumerConfiguration.valueSerde)
      .mapM { record =>
        val key = record.key
        val value = record.value
        fn(key, value)
      }
      .provideSomeLayer(consumer)
      .runDrain.retry(scheduleEvery5Seconds)
  }

}

case class KafkaConsumerConfiguration[K: Tag, V: Tag](topic: String, consumerSettings: ConsumerSettings,
                                                      keySerde: Serde[Any, K],
                                                      valueSerde: Serde[Any, V] )


object KafkaConsumer {
  def apply[K: Tag, V: Tag](consumerConfiguration: KafkaConsumerConfiguration[K, V]): KafkaConsumer[K, V] =
    new KafkaConsumer(consumerConfiguration)
}


class KafkaProducer[K: Tag, V: Tag](
                                   topic: String,
                                   producerSettings: ProducerSettings,
                                   keySerde: Serde[Any, K],
                                   valueSerde: Serde[Any, V]
                                 ) {
  type RecordProducer = Producer[Any, K, V]

  val producerManaged: ZManaged[Any, Throwable, Producer.Service[Any, K, V]] =
    Producer.make[Any, K, V](producerSettings, keySerde, valueSerde)

  val producer: ZLayer[Blocking, Throwable, RecordProducer] = ZLayer.fromManaged(producerManaged)

  def produce(key: K, value: V): ZIO[Blocking, Throwable, RecordMetadata] = {
    Producer.produce[Any, K, V](new ProducerRecord(topic, key, value)).provideSomeLayer(producer)
  }

}



case class KafkaProducerConfiguration(topic: String, producerSettings: ProducerSettings)
