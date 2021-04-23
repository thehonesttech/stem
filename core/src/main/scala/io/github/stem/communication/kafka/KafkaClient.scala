package io.github.stem.communication.kafka

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

class KafkaMessageConsumer[K: Tag, V: Tag, Reject: Tag](
  consumerConfiguration: KafkaConsumerConfig[K, V],
  errorHandler: Throwable => Reject,
  logic: (K, V) => IO[Reject, Unit]
) extends MessageConsumer[K, V, Reject] {
  type RecordProducer = Producer[Any, K, V]
  val consumerManaged: ZManaged[Clock with Blocking, Throwable, Consumer.Service] =
    Consumer.make(consumerConfiguration.consumerSettings)

  val consumer: ZLayer[Clock with Blocking, Throwable, Consumer] =
    ZLayer.fromManaged(consumerManaged)

  val messageStream: ZStream[Clock with Blocking, Reject, Unit] = {
    Consumer
      .subscribeAnd(Subscription.topics(consumerConfiguration.topic))
      .plainStream(consumerConfiguration.keySerde, consumerConfiguration.valueSerde)
      .provideSomeLayer(consumer)
      .mapError(errorHandler)
      .mapM { record =>
        val key = record.key
        val value = record.value
        ZIO.accessM[Clock with Blocking](_ => logic(key, value))
      }
  }
}

trait MessageConsumerSubscriber {
  def consumeForever: UIO[SubscriptionKillSwitch]
}

object MessageConsumerSubscriber {

  def consumeForever: ZIO[Has[MessageConsumerSubscriber], Nothing, SubscriptionKillSwitch] = ZIO.accessM[Has[MessageConsumerSubscriber]](_.get.consumeForever)

  val live =
    ZLayer.fromServices[ZStream[Clock with Blocking, String, Unit], Clock.Service, Console.Service, Blocking.Service, MessageConsumerSubscriber] {
      (messageStream, clock, console, blocking) =>
        new MessageConsumerSubscriber {
          private val scheduleEvery5Seconds = Schedule
            .spaced(5.seconds)
            .onDecision({
              case Decision.Done(_)                 => console.putStrLn(s"Reconnection successfull")
              case Decision.Continue(attempt, _, _) => console.putStrLn(s"Error while reconnecting attempt #$attempt")
            })

          val consumeForever: UIO[SubscriptionKillSwitch] = {
            val killSwitch = SubscriptionKillSwitch(Task.unit)
            messageStream
              .interruptWhen(killSwitch.shutdown)
              .runDrain
              .retry(scheduleEvery5Seconds)
              .fork
              .as(killSwitch)
              .provideLayer(ZLayer.succeed(clock) and ZLayer.succeed(blocking) and ZLayer.succeed(blocking))
          }
        }
    }
}

case class SubscriptionKillSwitch(shutdown: Task[Unit])

trait MessageConsumer[K, V, Reject] {

  def messageStream: ZStream[Clock with Blocking, Reject, Unit]

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

object KafkaMessageConsumer {
  def apply[K: Tag, V: Tag, Reject: Tag](
    consumerConfiguration: KafkaConsumerConfig[K, V],
    errorHandler: Throwable => Reject,
    logic: (K, V) => IO[Reject, Unit]
  ): KafkaMessageConsumer[K, V, Reject] =
    new KafkaMessageConsumer(consumerConfiguration, errorHandler, logic)
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
