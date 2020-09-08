package ledger

import akka.actor.ActorSystem
import io.grpc.Status
import ledger.Converters._
import ledger.InboundMessageHandling.ConsumerConfiguration
import ledger.LedgerEntity.{tagging, LedgerCommandHandler}
import ledger.LedgerGrpcService.Ledgers
import ledger.communication.grpc.service.ZioService.ZLedger
import ledger.communication.grpc.service._
import ledger.eventsourcing.events.events.{AmountLocked, LedgerEvent, LockReleased}
import ledger.messages.messages.{Authorization, LedgerId, LedgerInstructionsMessage, LedgerInstructionsMessageMessage}
import scalapb.zio_grpc.{ServerMain, ServiceList}
import stem.StemApp
import stem.communication.kafka.{KafkaConsumer, KafkaConsumerConfig, KafkaGrpcConsumerConfiguration, MessageConsumer, SubscriptionKillSwitch}
import stem.communication.macros.RpcMacro
import stem.communication.macros.annotations.MethodId
import stem.data._
import stem.journal.EventJournal
import stem.readside.ReadSideProcessing
import stem.runtime.Fold
import stem.runtime.akka.StemRuntime.memoryStemtity
import stem.runtime.akka._
import stem.runtime.readside.CommittableJournalQuery
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.duration.{durationInt, Duration}
import zio.kafka.consumer.ConsumerSettings
import zio.{console, Has, Runtime, Task, ULayer, ZEnv, ZIO, ZLayer}

sealed trait LockResponse

case object Allowed extends LockResponse

case class Denied(reason: String) extends LockResponse

object LedgerServer extends ServerMain {

  type LedgerCombinator = AlgebraCombinators[Int, LedgerEvent, String]
  val readSidePollingInterval: Duration = 100.millis
  private val kafkaConfiguration: ULayer[Has[ConsumerConfiguration]] =
    ZLayer.succeed(
      KafkaGrpcConsumerConfiguration[LedgerId, LedgerInstructionsMessage, LedgerInstructionsMessageMessage](
        "testtopic",
        ConsumerSettings(List("0.0.0.0"))
      )
    )
  private val stemRuntimeLayer = StemApp.stemStores[String, LedgerEvent]() ++ StemApp.actorSettings("System")

  private val ledgerEntity = stemRuntimeLayer to LedgerEntity.live
  private val kafkaConsumer: ZLayer[Any, Nothing, Has[MessageConsumer[LedgerId, LedgerInstructionsMessage]]] = kafkaConfiguration >>> ZIO
      .access[Has[ConsumerConfiguration]](layer => KafkaConsumer(layer.get))
      .toLayer
  private val kafkaMessageHandling = ZEnv.live and kafkaConsumer and ledgerEntity to InboundMessageHandling.live
  private val readSideProcessing = stemRuntimeLayer to ReadSideProcessing.live
  private val readSideProcessor = (ZEnv.live and readSideProcessing and stemRuntimeLayer) to LedgerReadSideProcessor.live
  private val ledgerService = ledgerEntity to LedgerGrpcService.live

  private def buildSystem[R]: ZLayer[R, Throwable, Has[ZLedger[ZEnv, Any]]] =
    (ledgerService and kafkaMessageHandling and readSideProcessor).mapError(_ => new RuntimeException("Bad layer"))

  override def services: ServiceList[zio.ZEnv] = ServiceList.addManaged(buildSystem.build.map(_.get))
}

// you can have multiples entities
object LedgerEntity {
  implicit val runtime: Runtime[zio.ZEnv] = LedgerServer
  implicit val keyEncoder: KeyEncoder[String] = (a: String) => a
  implicit val keyDecoder: KeyDecoder[String] = (key: String) => Some(key)

  class LedgerCommandHandler {
    type SIO[Response] = StemApp.SIO[Int, LedgerEvent, String, Response]

    @MethodId(1)
    def lock(amount: BigDecimal, idempotencyKey: String): SIO[LockResponse] = ZIO.accessM { opsL =>
      val ops = opsL.get
      import ops._
      (for {
        _     <- Task.unit
        state <- read
        _     <- append(AmountLocked(amount = toLedgerBigDecimal(amount), idempotencyKey = idempotencyKey))
      } yield Allowed).mapError(errorHandler)
    }

    @MethodId(2)
    def release(transactionId: String, idempotencyKey: String): SIO[Unit] = ???

    @MethodId(3)
    def clear(transactionId: String, idempotencyKey: String): SIO[Unit] = ???

  }

  val errorHandler: Throwable => String = error => error.getMessage

  val eventHandlerLogic: Fold[Int, LedgerEvent] = Fold(
    initial = 0,
    reduce = {
      case (oldState, event) =>
        val newState = event match {
          case _: AmountLocked =>
            oldState + 1
          case _: LockReleased => 20
          case _               => 30
        }
        Task.succeed(newState)
    }
  )

  implicit val ledgerProtocol: StemProtocol[LedgerCommandHandler, Int, LedgerEvent, String] =
    RpcMacro.derive[LedgerCommandHandler, Int, LedgerEvent, String]

  val tagging = Tagging.const(EventTag("Ledger"))

  val stemtity = memoryStemtity[String, LedgerCommandHandler, Int, LedgerEvent, String](
    "Ledger",
    tagging,
    EventSourcedBehaviour(new LedgerCommandHandler(), eventHandlerLogic, errorHandler)
  )

  val live: ZLayer[Has[ActorSystem] with Has[RuntimeSettings] with Has[EventJournal[String, LedgerEvent]], Throwable, Has[Ledgers]] = stemtity.toLayer
}

object LedgerReadSideProcessor {

  implicit val runtime: Runtime[ZEnv] = LedgerServer

  private val task: ZIO[Console, Nothing, (String, LedgerEvent) => Task[Unit]] = ZIO.access { layer =>
    val cons = layer.get
    (key: String, event: LedgerEvent) => {
      cons.putStrLn(s"Arrived $key")
    }
  }

  val live: ZLayer[Console with Clock with Has[ReadSideProcessing] with Has[CommittableJournalQuery[Long, String, LedgerEvent]], Throwable, Has[
    ReadSideProcessing.KillSwitch
  ]] = {
    ZLayer.fromAcquireRelease(for {
      readSideLogic <- task
      readSide <- StemApp
        .readSide[String, LedgerEvent, Long](
          "LedgerReadSide",
          ConsumerId("processing"),
          tagging,
          30,
          readSideLogic
        )
    } yield readSide)(killSwitch => killSwitch.shutdown.exitCode)
  }
}

object InboundMessageHandling {

  import StemApp.Ops._

  type ConsumerConfiguration = KafkaConsumerConfig[LedgerId, LedgerInstructionsMessage]
  type LedgerMessageConsumer = MessageConsumer[LedgerId, LedgerInstructionsMessage]

  val messageHandling: ZIO[Has[Ledgers], Throwable, (LedgerId, LedgerInstructionsMessage) => Task[Unit]] =
    ZIO.access { layers =>
      val ledgers = layers.get
      (key: LedgerId, instructionMessage: LedgerInstructionsMessage) => {
        instructionMessage match {
          case Authorization(accountId, amount, idempotencyKey, _) =>
            ledgers(accountId)
              .lock(fromLedgerBigDecimal(amount), idempotencyKey)
              .as()
              .provideCombinator
              .mapError(error => new Exception(s"$error happened"))
          case _ => ZIO.unit
        }
      }
    }

  val subscription: ZIO[Clock with Blocking with Console with Has[Ledgers] with Has[LedgerMessageConsumer], Throwable, SubscriptionKillSwitch] =
    ZIO.accessM { layers =>
      val messageConsumer = layers.get[MessageConsumer[LedgerId, LedgerInstructionsMessage]]
      messageHandling.flatMap(messageConsumer.subscribe)
    }

  val live: ZLayer[Clock with Blocking with Console with Has[Ledgers] with Has[LedgerMessageConsumer], Throwable, Has[
    SubscriptionKillSwitch
  ]] = ZLayer.fromAcquireRelease(subscription)(ks => ks.shutdown.exitCode)
}

object LedgerGrpcService {

  import Converters.Ops._
  import StemApp.Ops._
  type Ledgers = String => LedgerCommandHandler

  val service: ZIO[Has[Ledgers], Nothing, ZioService.ZLedger[ZEnv, Any]] = ZIO.access { layer =>
    val ledgers = layer.get
    new ZioService.ZLedger[ZEnv, Any] {

      override def lock(request: LockRequest): ZIO[ZEnv, Status, LockReply] = {
        (for {
          reply <- ledgers(request.id)
            .lock(request.amount, request.idempotencyKey)
            .provideCombinator
          _ <- console.putStrLn(reply.toString)
        } yield LockReply().withMessage(reply.toString)).orElseFail(Status.NOT_FOUND)
      }

      override def release(request: ReleaseRequest): ZIO[ZEnv, Status, ReleaseReply] =
        ???

      override def clear(request: ClearRequest): ZIO[ZEnv, Status, ClearReply] = ???
    }
  }

  val live = service.toLayer

}
