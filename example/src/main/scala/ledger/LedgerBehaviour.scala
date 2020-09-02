package ledger

import akka.actor.ActorSystem
import io.grpc.Status
import ledger.Converters._
import ledger.LedgerEntity.{tagging, LedgerCommandHandler}
import ledger.LedgerGrpcService.Ledgers
import ledger.InboundMessageHandling.ConsumerConfiguration
import ledger.communication.grpc.service.ZioService.ZLedger
import ledger.communication.grpc.service._
import ledger.eventsourcing.events.events.{AmountLocked, LedgerEvent, LockReleased}
import ledger.messages.messages.{Authorization, LedgerId, LedgerInstructionsMessage, LedgerInstructionsMessageMessage}
import scalapb.zio_grpc.{ServerMain, ServiceList}
import stem.StemApp
import stem.communication.kafka.{KafkaConsumer, KafkaConsumerConfig, KafkaGrpcConsumerConfiguration}
import stem.communication.macros.RpcMacro
import stem.communication.macros.annotations.MethodId
import stem.data._
import stem.journal.EventJournal
import stem.runtime.akka.StemRuntime.memoryStemtity
import stem.runtime.akka._
import stem.runtime.readside.JournalStores
import stem.runtime.{AlgebraTransformer, Fold}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.kafka.consumer.ConsumerSettings
import zio.{Has, Runtime, Task, ULayer, ZEnv, ZIO, ZLayer}

sealed trait LockResponse

case object Allowed extends LockResponse

case class Denied(reason: String) extends LockResponse

object LedgerServer extends ServerMain {

  import JournalStores._

  type LedgerCombinator = AlgebraCombinators[Int, LedgerEvent, String]
  private val actorSystem = StemApp.actorSystemLayer("System")
  private val runtimeSettings = actorSystem to ZLayer.fromService(RuntimeSettings.default)
  private val (eventJournalStore, committableJournalQueryStore) = memoryJournalAndQueryStoreLayer[String, LedgerEvent]

  private val kafkaConfiguration: ULayer[Has[ConsumerConfiguration]] =
    ZLayer.succeed(
      KafkaGrpcConsumerConfiguration[LedgerId, LedgerInstructionsMessage, LedgerInstructionsMessageMessage](
        "testtopic",
        ConsumerSettings(List("0.0.0.0"))
      )
    )

  private val ledgerEntity = (actorSystem and runtimeSettings and eventJournalStore to LedgerEntity.live)
  private val kafkaMessageHandling = ZEnv.live and kafkaConfiguration and ledgerEntity to InboundMessageHandling.liveLayer
  private val readSideProcessing = (ZEnv.live and actorSystem and committableJournalQueryStore and ledgerEntity) to ReadSideProcessor.live
  private val ledgerService = ledgerEntity to LedgerGrpcService.live

  private def buildSystem[R]: ZLayer[R, Throwable, Has[ZLedger[ZEnv, Any]]] =
    ledgerService and kafkaMessageHandling and readSideProcessing

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

  val stemtity = memoryStemtity[String, LedgerCommandHandler, Int, LedgerEvent, String](
    "Ledger",
    tagging,
    EventSourcedBehaviour(new LedgerCommandHandler(), eventHandlerLogic, errorHandler))

  implicit val ledgerProtocol: StemProtocol[LedgerCommandHandler, Int, LedgerEvent, String] =
    RpcMacro.derive[LedgerCommandHandler, Int, LedgerEvent, String]

  val tagging = Tagging.const(EventTag("Ledger"))

  val live: ZLayer[Has[ActorSystem] with Has[RuntimeSettings] with Has[EventJournal[String, LedgerEvent]], Throwable, Has[Ledgers]] = stemtity.toLayer
}

object ReadSideProcessor {

  implicit val runtime: Runtime[ZEnv] = LedgerServer

  val live = ZIO.accessM { ledgers: Has[Ledgers] =>
    val processor = new LedgerProcessor(ledgers.get)
    val consumerId = ConsumerId("processing")
    StemApp.readSide[String, LedgerEvent, Long]("LedgerReadSide", consumerId, tagging, processor.process)
  }.toLayer

  final class LedgerProcessor(ledgers: Ledgers) {
    def process(key: String, ledgerEvent: LedgerEvent): Task[Unit] = {
      // for now just print console
      ???
    }
  }

}

object InboundMessageHandling {

  import StemApp.Ops._

  type ConsumerConfiguration = KafkaConsumerConfig[LedgerId, LedgerInstructionsMessage]

  val messageHandling: ZIO[Has[Ledgers], Throwable, (LedgerId, LedgerInstructionsMessage) => Task[Unit]] =
    ZIO.access { layers =>
      val ledgers = layers.get
      (key: LedgerId, instructionMessage: LedgerInstructionsMessage) =>
        {
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

  val subscription: ZIO[Clock with Blocking with Console with Has[Ledgers] with Has[ConsumerConfiguration], Throwable, Unit] = ZIO.accessM { layers =>
    val kafkaConsumerConfiguration = layers.get[ConsumerConfiguration]
    messageHandling.flatMap(KafkaConsumer(kafkaConsumerConfiguration).subscribe)
  }

  val liveLayer: ZLayer[Clock with Blocking with Console with Has[Ledgers] with Has[ConsumerConfiguration], Throwable, Has[
    Unit
  ]] = subscription.toLayer
}

object LedgerGrpcService {

  import StemApp.Ops._
  import Converters.Ops._
  type Ledgers = String => LedgerCommandHandler

  val service: ZIO[Has[Ledgers],Nothing,  ZioService.ZLedger[ZEnv, Any]] =  ZIO.access{ layer =>
    val ledgers = layer.get
    new ZioService.ZLedger[ZEnv, Any] {

      import zio.console._

      override def lock(request: LockRequest): ZIO[ZEnv, Status, LockReply] = {
        (for {
          reply <- ledgers(request.id)
            .lock(request.amount, request.idempotencyKey).provideCombinator
          _ <- putStrLn(reply.toString)
        } yield LockReply().withMessage(reply.toString)).orElseFail(Status.NOT_FOUND)
      }

      override def release(request: ReleaseRequest): ZIO[ZEnv, Status, ReleaseReply] =
        ???

      override def clear(request: ClearRequest): ZIO[ZEnv, Status, ClearReply] = ???
    }
  }

  val live = service.toLayer

}
