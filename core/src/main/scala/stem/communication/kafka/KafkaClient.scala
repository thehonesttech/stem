package stem.communication.kafka

import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.header.Headers
import scalapb.{GeneratedMessage, GeneratedMessageCompanion, GeneratedSealedOneof, TypeMapper}
import zio.Schedule.Decision
import zio.{Tag, ZLayer, _}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.{putStrLn, Console}
import zio.duration.durationInt
import zio.kafka.consumer.{Consumer, ConsumerSettings, Subscription}
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde._
import zio.stream.ZStream

class KafkaConsumer[K: Tag, V: Tag](
  consumerConfiguration: KafkaConsumerConfig[K, V]
) extends MessageConsumer[K, V] {
  type RecordProducer = Producer[Any, K, V]
  val consumerManaged: ZManaged[Clock with Blocking, Throwable, Consumer.Service] =
    Consumer.make(consumerConfiguration.consumerSettings)

  val consumer: ZLayer[Clock with Blocking, Throwable, Consumer] =
    ZLayer.fromManaged(consumerManaged)

  private val scheduleEvery5Seconds = Schedule
    .spaced(5.seconds)
    .onDecision({
      case Decision.Done(_)                 => putStrLn(s"Reconnection successfull")
      case Decision.Continue(attempt, _, _) => putStrLn(s"Error while reconnecting attempt #$attempt")
    })

  def messageStream(fn: (K, V) => Task[Unit]): ZStream[Clock with Blocking, Throwable, Unit] = {
    Consumer
      .subscribeAnd(Subscription.topics(consumerConfiguration.topic))
      .plainStream(consumerConfiguration.keySerde, consumerConfiguration.valueSerde)
      .provideSomeLayer(consumer)
      .mapM { record =>
        val key = record.key
        val value = record.value
        fn(key, value)
      }
  }

  def subscribe(fn: (K, V) => Task[Unit]): ZIO[Clock with Blocking with Console, Throwable, SubscriptionKillSwitch] = {
    val shutdown = Task.unit
    messageStream(fn)
      .interruptWhen(shutdown)
      .runDrain
      .retry(scheduleEvery5Seconds)
      .as(SubscriptionKillSwitch(shutdown))
  }
}

case class SubscriptionKillSwitch(shutdown: Task[Unit])

trait MessageConsumer[K, V] {

  def messageStream(fn: (K, V) => Task[Unit]): ZStream[Clock with Blocking, Throwable, Unit]

  // TODO do not override subscribe but only messageStream
  def subscribe(fn: (K, V) => Task[Unit]): ZIO[Clock with Blocking with Console, Throwable, SubscriptionKillSwitch]

}

trait KafkaConsumerConfig[K, V] {
  def topic: String

  def consumerSettings: ConsumerSettings

  def keySerde: Serde[Any, K]

  def valueSerde: Serde[Any, V]
}

case class KafkaConsumerConfiguration[K: Tag, V: Tag](
  topic: String,
  consumerSettings: ConsumerSettings,
  keySerde: Serde[Any, K],
  valueSerde: Serde[Any, V]
) extends KafkaConsumerConfig[K, V]

case class KafkaGrpcConsumerConfiguration[
  K <: GeneratedMessage: GeneratedMessageCompanion: Tag,
  V <: GeneratedSealedOneof: Tag,
  L <: GeneratedMessage: GeneratedMessageCompanion
](topic: String, consumerSettings: ConsumerSettings)(implicit val typeMapper: TypeMapper[L, V])
    extends KafkaConsumerConfig[K, V] {
  val keySerde: Serde[Any, K] = new GrpcSerde[K]
  val valueSerde: Serde[Any, V] = new GrpcSealedSerde[V, L]
}

class GrpcSealedSerde[T <: GeneratedSealedOneof, L <: GeneratedMessage: GeneratedMessageCompanion](implicit val typeMapper: TypeMapper[L, T])
    extends Serde[Any, T] {
  def deserialize(topic: String, headers: Headers, data: Array[Byte]): RIO[Any, T] = {
    RIO.succeed(implicitly[TypeMapper[L, T]].toCustom(implicitly[GeneratedMessageCompanion[L]].parseFrom(data)))
  }

  def serialize(topic: String, headers: Headers, value: T): RIO[Any, Array[Byte]] = {
    RIO.succeed(implicitly[GeneratedMessageCompanion[L]].toByteArray(implicitly[TypeMapper[L, T]].toBase(value)))
  }

  def configure(props: Map[String, AnyRef], isKey: Boolean): Task[Unit] = Task.unit
}

class GrpcSerde[T <: GeneratedMessage: GeneratedMessageCompanion] extends Serde[Any, T] {
  def deserialize(topic: String, headers: Headers, data: Array[Byte]): RIO[Any, T] =
    RIO.succeed(implicitly[GeneratedMessageCompanion[T]].parseFrom(data))

  def serialize(topic: String, headers: Headers, value: T): RIO[Any, Array[Byte]] =
    RIO.succeed(implicitly[GeneratedMessageCompanion[T]].toByteArray(value))

  def configure(props: Map[String, AnyRef], isKey: Boolean): Task[Unit] = Task.unit
}

object KafkaConsumer {
  def apply[K: Tag, V: Tag](consumerConfiguration: KafkaConsumerConfig[K, V]): KafkaConsumer[K, V] =
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
