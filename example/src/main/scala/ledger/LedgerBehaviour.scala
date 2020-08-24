package ledger

import akka.actor.ActorSystem
import io.grpc.Status
import ledger.LedgerEntity.{LedgerCommandHandler, tagging}
import ledger.LedgerService.Ledgers
import ledger.MessageHandler.ConsumerConfiguration
import ledger.communication.grpc.service.ZioService.ZLedger
import ledger.communication.grpc.service._
import ledger.eventsourcing.events.events
import ledger.eventsourcing.events.events.{AmountLocked, LedgerEvent, LockReleased}
import ledger.messages.messages.{Authorization, LedgerId, LedgerInstructionsMessage, LedgerInstructionsMessageMessage}
import scalapb.zio_grpc.{ServerMain, ServiceList}
import stem.StemApp
import stem.communication.kafka.{KafkaConsumer, KafkaConsumerConfig, KafkaGrpcConsumerConfiguration}
import stem.communication.macros.RpcMacro
import stem.communication.macros.annotations.MethodId
import stem.data.AlgebraCombinators.Combinators
import stem.data.{AlgebraCombinators, ConsumerId, StemProtocol}
import stem.journal.EventJournal
import stem.runtime.akka.StemRuntime.memoryStemtity
import stem.runtime.akka._
import stem.runtime.readside.{CommittableJournalQuery, CommittableJournalStore, JournalQuery}
import stem.runtime.{AlgebraTransformer, EventJournalStore, Fold, KeyValueStore}
import stem.snapshot.KeyValueStore
import stem.tagging.{EventTag, Tagging}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.kafka.consumer.ConsumerSettings
import zio.{Has, Runtime, Task, ULayer, ZEnv, ZIO, ZLayer}

sealed trait LockResponse

case object Allowed extends LockResponse

case class Denied(reason: String) extends LockResponse

object LedgerServer extends ServerMain {

  type LedgerCombinator = AlgebraCombinators[Int, LedgerEvent, String]
  private val actorSystem = StemApp.actorSystemLayer("System")
  private val runtimeSettings = actorSystem to ZLayer.fromService(RuntimeSettings.default)
  private val liveAlgebra = StemApp.liveAlgebraLayer[Int, LedgerEvent, String]

  // dependency injection wiring
  private val memoryStore = EventJournalStore.memory[String, LedgerEvent]

  private val readSideOffsetStore = ZLayer.fromEffect{
    KeyValueStore.memory[String, Long]
  }
  private val eventJournalStore = ZLayer.fromEffect(memoryStore.map[EventJournal[String, LedgerEvent]](identity))
  private val journalQueryStore = ZLayer.fromEffect(memoryStore.map[JournalQuery[Long, String, LedgerEvent]](identity))
  private val committableJournalQueryStore =  ZLayer.fromServices{
    (offsetStore: KeyValueStore[String, Long], journal: JournalQuery[Long, String, LedgerEvent]) => new CommittableJournalStore[Long, String, LedgerEvent ](offsetStore, journal) :CommittableJournalQuery[Long, String, LedgerEvent]
  }
  private val entity = (actorSystem and runtimeSettings and eventJournalStore to LedgerEntity.live)

  private val kafkaConfiguration: ULayer[Has[ConsumerConfiguration]] =
    ZLayer.succeed(
      KafkaGrpcConsumerConfiguration[LedgerId, LedgerInstructionsMessage, LedgerInstructionsMessageMessage](
        "testtopic",
        ConsumerSettings(List("0.0.0.0"))
      )
    )

  private val kafkaMessageHandling = ZEnv.live and kafkaConfiguration and entity and liveAlgebra to MessageHandler.live
  private val readSideProcessing = (readSideOffsetStore and journalQueryStore to committableJournalQueryStore and entity) to ReadSideProcessor.live

  private def buildSystem[R]: ZLayer[R, Throwable, Has[ZLedger[ZEnv, Any]]] =
    (entity and liveAlgebra to LedgerService.live) and kafkaMessageHandling and readSideProcessing

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
        state <- read
        _ <- append(AmountLocked(amount = Some(toLedgerBigDecimal(amount)), idempotencyKey = idempotencyKey))
      } yield Allowed).mapError(errorHandler)
    }

    @MethodId(2)
    def release(transactionId: String, idempotencyKey: String): SIO[Unit] = ???

    @MethodId(3)
    def clear(transactionId: String, idempotencyKey: String): SIO[Unit] = ???

    private def toLedgerBigDecimal(bigDecimal: BigDecimal): events.BigDecimal =
      ledger.eventsourcing.events.events.BigDecimal(bigDecimal.scale, bigDecimal.precision)
  }

  private val errorHandler: Throwable => String = error => error.getMessage

  val eventHandlerLogic: Fold[Int, LedgerEvent] = Fold(
    initial = 0,
    reduce = {
      case (oldState, event) =>
        val newState = event match {
          case _: AmountLocked =>
            if (oldState % 2 == 0) oldState + 1
            else oldState + 5
          case _: LockReleased => 2
          case _ => 3
        }
        Task.succeed(newState)
    }
  )

  implicit val ledgerProtocol: StemProtocol[LedgerCommandHandler, Int, LedgerEvent, String] =
    RpcMacro.derive[LedgerCommandHandler, Int, LedgerEvent, String]

  val tagging = Tagging.const(EventTag("Ledger"))

  // TODO: setup kafka
  val live: ZLayer[Has[ActorSystem] with Has[RuntimeSettings] with Has[EventJournal[String, LedgerEvent]], Throwable, Has[String => LedgerCommandHandler]] = ZLayer.fromEffect {
    memoryStemtity[String, LedgerCommandHandler, Int, LedgerEvent, String](
      "Ledger",
      tagging,
      EventSourcedBehaviour(new LedgerCommandHandler(), eventHandlerLogic, errorHandler)
    )
  }
}

object ReadSideProcessor {
  def live =
    ZLayer.fromServices { (journalQuery: CommittableJournalQuery[Long, String, LedgerEvent], ledgers: Ledgers) =>
      val processor = new LedgerProcessor(ledgers)
      val consumerId = ConsumerId("processing")
      tagging.tags.map { tag =>
        journalQuery.eventsByTag(tag, consumerId)

      }
      ()

    }

  private def readSideProcessorLogic(ledgers: Ledgers) = {

    ???
  }

  final class LedgerProcessor(ledgers: Ledgers) {
    def process(ledgerEvent: LedgerEvent): Task[Unit] = {
      ???
    }
  }


}


object MessageHandler {

  import LedgerServer.LedgerCombinator
  import ledger.LedgerService.Conversions._

  type ConsumerConfiguration = KafkaConsumerConfig[LedgerId, LedgerInstructionsMessage]

  val messageHandling: ZIO[Has[Ledgers] with Has[LedgerCombinator], Throwable, (LedgerId, LedgerInstructionsMessage) => Task[Unit]] =
    ZIO.access { layers =>
      val ledgers = layers.get
      val combinator: LedgerCombinator = layers.get[LedgerCombinator]
      (key: LedgerId, instructionMessage: LedgerInstructionsMessage) => {
        instructionMessage match {
          case Authorization(accountId, amount, idempotencyKey, _) =>
            ledgers(accountId)
              .lock(fromLedgerBigDecimal(amount), idempotencyKey)
              .as()
              .mapError(error => new Exception(s"$error happened"))
          case _ => ZIO.unit
        }
      }.provideLayer(ZLayer.succeed(combinator))
    }

  val live
  : ZLayer[Clock with Blocking with Console with Has[Ledgers] with Has[LedgerCombinator] with Has[ConsumerConfiguration], Throwable, Has[
    Unit
  ]] = {
    ZLayer.fromEffect {
      ZIO.accessM { layers: Has[ConsumerConfiguration] =>
        val kafkaConsumerConfiguration = layers.get[ConsumerConfiguration]
        messageHandling.flatMap(handling => KafkaConsumer(kafkaConsumerConfiguration).subscribe(handling))
      }
    }
  }
}

object LedgerService {

  import AlgebraTransformer.Ops._

  type Ledgers = String => LedgerCommandHandler

  object Conversions {

    implicit def toLedgerBigDecimal(bigDecimal: BigDecimal): events.BigDecimal =
      ledger.eventsourcing.events.events.BigDecimal(bigDecimal.scale, bigDecimal.precision)

    implicit def fromLedgerBigDecimal(bigDecimal: Option[events.BigDecimal]): BigDecimal = {
      bigDecimal.map(el => BigDecimal.apply(el.scale, el.precision)).getOrElse(BigDecimal(0))
    }
  }

  val live =
    ZLayer.fromServices { (ledgers: Ledgers, algebra: AlgebraCombinators[Int, LedgerEvent, String]) =>
      new ZioService.ZLedger[ZEnv with Combinators[Int, LedgerEvent, String], Any] {

        import Conversions._
        import zio.console._

        override def lock(request: LockRequest): ZIO[ZEnv with Combinators[Int, LedgerEvent, String], Status, LockReply] = {
          (for {
            reply <- ledgers(request.id)
              .lock(request.amount, request.idempotencyKey)
            _ <- putStrLn(reply.toString)
          } yield LockReply().withMessage(reply.toString))
            .mapError(_ => Status.NOT_FOUND)
        }

        override def release(request: ReleaseRequest): ZIO[ZEnv with Combinators[Int, LedgerEvent, String], Status, ReleaseReply] =
          ???

        override def clear(request: ClearRequest): ZIO[ZEnv with Combinators[Int, LedgerEvent, String], Status, ClearReply] = ???
      }.withAlgebra(algebra)
    }

}
