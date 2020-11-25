package ledger

import accounts.AccountEntity.{errorHandler, tagging, AccountCommandHandler}
import accounts.{AccountEntity, AccountId, AccountTransactionId}
import io.grpc.Status
import ledger.LedgerGrpcService.{Accounts, Transactions}
import ledger.LedgerServer.emptyCombinators
import ledger.communication.grpc.ZioService.ZLedger
import ledger.communication.grpc._
import ledger.eventsourcing.events.{AccountEvent, AccountState, TransactionAuthorized, TransactionCreated, TransactionEvent, TransactionState}
import ledger.messages.messages.{AuthorizePaymentMessage, LedgerId, LedgerInstructionsMessage, LedgerInstructionsMessageMessage, OpenAccountMessage}
import scalapb.zio_grpc.{ServerMain, ServiceList}
import io.github.stem.StemApp
import io.github.stem.StemApp.{clientEmptyCombinator, ReadSideParams}
import io.github.stem.communication.kafka._
import io.github.stem.data._
import io.github.stem.readside.ReadSideProcessing
import io.github.stem.runtime.readside.CommittableJournalQuery
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

  private val ledgerLogicLayer = entities to ProcessReadSide.live

  private val readSideProcessor = ledgerLogicLayer and runtimeLayers to TransactionReadSideProcessor.live

  private val kafkaMessageConsumer = entities to
    LedgerInboundMessageHandling.messageHandling
      .map(logic => KafkaMessageConsumer(kafkaConfiguration, errorHandler, logic): MessageConsumer[LedgerId, LedgerInstructionsMessage, String])
      .toLayer

  private val kafkaMessageHandling = ZEnv.live and kafkaMessageConsumer to LedgerInboundMessageHandling.live

  private val ledgerService = (entities and ledgerLogicLayer) to LedgerGrpcService.live

  private def buildSystem[R]: ZLayer[R, Throwable, Has[ZLedger[ZEnv, Any]]] =
    (ledgerService and kafkaMessageHandling and readSideProcessor).mapError(_ => new RuntimeException("Bad layer"))

  val emptyCombinators: ZLayer[Any, Nothing, Has[AlgebraCombinators[AccountState, AccountEvent, String]] with Has[
    AlgebraCombinators[TransactionState, TransactionEvent, String]
  ]] = clientEmptyCombinator[AccountState, AccountEvent, String] ++ clientEmptyCombinator[TransactionState, TransactionEvent, String]

  override def services: ServiceList[zio.ZEnv] = ServiceList.addManaged(buildSystem.build.map(_.get))
}

class ProcessReadSide(accounts: Accounts, transactions: Transactions) {

  def process(transactionId: TransactionId, transactionEvent: TransactionEvent): IO[String, Unit] = {
    transactionEvent match {
      case TransactionCreated(from, to, amount) =>
        accounts(from)
          .debit(AccountTransactionId(transactionId.value), amount)
          .foldM(failReason => transactions(transactionId).fail(failReason), _ => transactions(transactionId).authorise)
          .provideLayer(LedgerServer.emptyCombinators)
      case TransactionAuthorized() =>
        (for {
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
        } yield creditResult)
          .provideLayer(LedgerServer.emptyCombinators)
      case _ => IO.fail("Unexpected message")
    }
  }
}

object ProcessReadSide {
  val live = ZLayer.fromServices[Accounts, Transactions, ProcessReadSide] { (accounts: Accounts, transactions: Transactions) =>
    new ProcessReadSide(accounts, transactions)
  }
}

object TransactionReadSideProcessor {

  implicit val runtime: Runtime[ZEnv] = LedgerServer

  val readsideParams: ZIO[Has[ProcessReadSide], Nothing, ReadSideParams[TransactionId, TransactionEvent, String]] =
    ZIO.access[Has[ProcessReadSide]] { layer =>
      ReadSideParams("TransactionReadSide", ConsumerId("transactionProcessing"), TransactionEntity.tagging, 30, layer.get[ProcessReadSide].process)
    }

  val live: ZLayer[Console with Clock with Has[ReadSideProcessing] with Has[CommittableJournalQuery[Long, TransactionId, TransactionEvent]] with Has[
    ProcessReadSide
  ], String, Has[
    ReadSideProcessing.KillSwitch
  ]] = {
    ZLayer.fromAcquireRelease(for {
      readSideParams <- readsideParams
      _              <- ZIO.service[Console.Service]
      killSwitch <- StemApp
        .readSideSubscription[TransactionId, TransactionEvent, Long, String](readSideParams, errorHandler)
    } yield killSwitch)(killSwitch => killSwitch.shutdown.exitCode)
  }
}

object LedgerInboundMessageHandling {

  type ConsumerConfiguration = KafkaConsumerConfig[LedgerId, LedgerInstructionsMessage]
  type LedgerMessageConsumer = MessageConsumer[LedgerId, LedgerInstructionsMessage, String]

  val messageHandling: ZIO[Has[Accounts] with Has[Transactions], Throwable, (LedgerId, LedgerInstructionsMessage) => IO[String, Unit]] =
    ZIO.access { layer =>
      val accounts = layer.get[Accounts]
      val transactions = layer.get[Transactions]
      (_: LedgerId, instructionMessage: LedgerInstructionsMessage) => {
        instructionMessage match {
          case OpenAccountMessage(accountId) =>
            accounts(accountId).open
              .provideLayer(emptyCombinators)
          case AuthorizePaymentMessage(transactionId, from, to, amount) =>
            transactions(transactionId)
              .create(from, to, amount)
              .provideLayer(emptyCombinators)
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

  private val service: ZIO[Has[Accounts] with Has[Transactions], Nothing, ZioService.ZLedger[ZEnv, Any]] = ZIO.access { layer =>
    val accounts = layer.get[Accounts]
    val transactions = layer.get[Transactions]
    new ZioService.ZLedger[ZEnv, Any] {
      override def openAccount(openAccountRequest: OpenAccountRequest): ZIO[Any, Status, OpenAccountReply] = {
        accounts(openAccountRequest.accountId).open
          .bimap(_ => Status.NOT_FOUND, _ => OpenAccountReply().withMessage("Created"))
          .provideLayer(emptyCombinators)
      }

      override def authorizePayment(authorizeRequest: AuthorizeRequest): ZIO[Any, Status, AuthorizeReply] = {
        transactions(authorizeRequest.transactionId)
          .create(authorizeRequest.from, authorizeRequest.to, authorizeRequest.amount)
          .bimap(_ => Status.NOT_FOUND, _ => AuthorizeReply().withMessage("Created"))
          .provideLayer(emptyCombinators)
      }
    }
  }

  val live: ZLayer[Has[Accounts] with Has[Transactions], Nothing, Has[ZLedger[ZEnv, Any]]] = service.toLayer

}
