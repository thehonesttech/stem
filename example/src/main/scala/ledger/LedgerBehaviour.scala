package ledger

import accounts.AccountEntity.{errorHandler, tagging, AccountCommandHandler}
import accounts.{AccountEntity, AccountId, AccountTransactionId}
import io.grpc.Status
import ledger.LedgerGrpcService.{Accounts, Transactions}
import ledger.LedgerServer.emptyCombinators
import ledger.communication.grpc.ZioService.ZLedger
import ledger.communication.grpc._
import ledger.eventsourcing.events.{AccountEvent, AccountState, TransactionEvent, TransactionState}
import ledger.messages.messages.{Authorization, Creation, LedgerId, LedgerInstructionsMessage, LedgerInstructionsMessageMessage}
import scalapb.zio_grpc.{ServerMain, ServiceList}
import stem.StemApp
import stem.StemApp.{clientEmptyCombinator, ReadSideParams}
import stem.communication.kafka._
import stem.data._
import stem.readside.ReadSideProcessing
import stem.runtime.readside.CommittableJournalQuery
import transactions.TransactionEntity.TransactionCommandHandler
import transactions.{TransactionEntity, TransactionId}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.duration.{durationInt, Duration}
import zio.kafka.consumer.ConsumerSettings
import zio.{Has, IO, Managed, Runtime, Task, ZEnv, ZIO, ZLayer}

sealed trait LockResponse

case object Allowed extends LockResponse

case class Denied(reason: String) extends LockResponse

object LedgerServer extends ServerMain {

  type LedgerCombinator = AlgebraCombinators[Int, AccountEvent, String]
  val readSidePollingInterval: Duration = 100.millis

  private val kafkaConfiguration = KafkaGrpcConsumerConfiguration[LedgerId, LedgerInstructionsMessage, LedgerInstructionsMessageMessage](
    "testtopic",
    ConsumerSettings(List("0.0.0.0"))
  )

  private val actorSystem = StemApp.actorSettings("System")
  private val runtimeLayers = actorSystem >+> (StemApp.liveRuntime[TransactionId, TransactionEvent] ++ StemApp.liveRuntime[AccountId, AccountEvent])
  private val entities = runtimeLayers to (AccountEntity.live ++ TransactionEntity.live)
  private val readSideProcessor = runtimeLayers to LedgerReadSideProcessor.live

  private val ledgerLogicLayer = entities to ZLayer.fromServices[Accounts, Transactions, LedgerLogic] { (accounts: Accounts, transactions: Transactions) =>
      new LedgerLogic(accounts, transactions)
    }

  private val kafkaMessageConsumer = ledgerLogicLayer to
    LedgerInboundMessageHandling.messageHandling
      .map(logic => KafkaMessageConsumer(kafkaConfiguration, errorHandler, logic): MessageConsumer[LedgerId, LedgerInstructionsMessage, String])
      .toLayer

  private val kafkaMessageHandling = ZEnv.live and kafkaMessageConsumer to LedgerInboundMessageHandling.live

  private val ledgerService = (entities and ledgerLogicLayer) to LedgerGrpcService.live

  private def buildSystem[R]: ZLayer[R, Throwable, Has[ZLedger[ZEnv, Any]]] =
    (ledgerService and kafkaMessageHandling and readSideProcessor).mapError(_ => new RuntimeException("Bad layer"))

  val emptyCombinators = clientEmptyCombinator[AccountState, AccountEvent, String] ++ clientEmptyCombinator[TransactionState, TransactionEvent, String]

  override def services: ServiceList[zio.ZEnv] = ServiceList.addManaged(buildSystem.build.map(_.get))
}

class LedgerLogic(accounts: Accounts, transactions: Transactions) {

  def create(transactionId: TransactionId, from: AccountId, amount: BigDecimal) = {
    accounts(from)
      .debit(AccountTransactionId(transactionId.value), amount)
      .foldM(transactions(transactionId).fail, _ => transactions(transactionId).authorise)
  }

  def authorize(transactionId: TransactionId) = {
    for {
      txn <- transactions(transactionId).getInfo
      creditResult <- accounts(txn.toAccountId)
        .credit(
          AccountTransactionId(transactionId.value),
          txn.amount
        )
        .foldM(
          { rejection =>
            // TODO better revert
            accounts(txn.fromAccountId).debit(
              AccountTransactionId(transactionId.value),
              txn.amount
            ) *> transactions(transactionId).fail(rejection)
          }, { _ =>
            transactions(transactionId).succeed
          }
        )
    } yield creditResult
  }

}

object LedgerReadSideProcessor {

  implicit val runtime: Runtime[ZEnv] = LedgerServer

  private val readSideLogic: ZIO[Console, Nothing, (AccountId, AccountEvent) => Task[Unit]] = ZIO.access { layer =>
    val cons = layer.get
    (key: AccountId, event: AccountEvent) => {
      cons.putStrLn(s"Arrived $key")
    }
  }
  val readSideParams = readSideLogic.map(logic => ReadSideParams("LedgerReadSide", ConsumerId("processing"), tagging, 30, logic))

  val live: ZLayer[Console with Clock with Has[ReadSideProcessing] with Has[CommittableJournalQuery[Long, AccountId, AccountEvent]], Throwable, Has[
    ReadSideProcessing.KillSwitch
  ]] = {
    ZLayer.fromAcquireRelease(for {
      readSideParams <- readSideParams
      killSwitch <- StemApp
        .readSideSubscription[AccountId, AccountEvent, Long](readSideParams)
    } yield killSwitch)(killSwitch => killSwitch.shutdown.exitCode)
  }
}

object LedgerInboundMessageHandling {

  type ConsumerConfiguration = KafkaConsumerConfig[LedgerId, LedgerInstructionsMessage]
  type LedgerMessageConsumer = MessageConsumer[LedgerId, LedgerInstructionsMessage, String]

  val messageHandling: ZIO[Has[LedgerLogic], Throwable, (LedgerId, LedgerInstructionsMessage) => IO[String, Unit]] =
    ZIO.access { layers =>
      val ledgerLogic = layers.get
      (key: LedgerId, instructionMessage: LedgerInstructionsMessage) => {
        instructionMessage match {
          case Creation(transactionId, accountId, amount) =>
            ledgerLogic
              .create(transactionId, accountId, amount)
              .as()
              .provideLayer(LedgerServer.emptyCombinators)
          case Authorization(transactionId) =>
            ledgerLogic
              .authorize(transactionId)
              .as()
              .provideLayer(LedgerServer.emptyCombinators)
          case _ => ZIO.unit
        }
      }
    }

  val live: ZLayer[Console with Clock with Blocking with Has[LedgerMessageConsumer], Nothing, Has[SubscriptionKillSwitch]] =
    ZLayer.fromManaged(
      Managed.make(
        for {
          stream     <- ZIO.service[LedgerMessageConsumer]
          service    <- MessageConsumerSubscriber.live.provide(stream.messageStream)
          killSwitch <- service.consumeForever
        } yield killSwitch
      )(_.shutdown.exitCode)
    )
}

object LedgerGrpcService {
  type Accounts = AccountId => AccountCommandHandler
  type Transactions = TransactionId => TransactionCommandHandler

  val service: ZIO[Has[LedgerLogic], Nothing, ZioService.ZLedger[ZEnv, Any]] = ZIO.access { layer =>
    val ledgerLogic = layer.get[LedgerLogic]
    new ZioService.ZLedger[ZEnv, Any] {

      override def create(request: CreateRequest): ZIO[ZEnv, Status, CreateReply] =
        ledgerLogic
          .create(request.transactionId, request.accountId, request.amount)
          .bimap(_ => Status.NOT_FOUND, _ => CreateReply().withMessage("Created"))
          .provideLayer(emptyCombinators)

      override def authorize(request: AuthorizeRequest): ZIO[ZEnv, Status, AuthorizeReply] = {
        ledgerLogic
          .authorize(request.transactionId)
          .bimap(_ => Status.NOT_FOUND, _ => AuthorizeReply().withMessage("Authorized"))
          .provideLayer(emptyCombinators)
      }
    }
  }

  val live = service.toLayer

}
