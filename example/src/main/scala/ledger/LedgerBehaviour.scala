package ledger

import accounts.AccountEntity.{errorHandler, AccountCommandHandler}
import accounts.{AccountEntity, AccountId, AccountTransactionId}
import io.github.stem.StemApp
import io.github.stem.StemApp.{clientEmptyCombinator, ReadSideParams}
import io.github.stem.communication.kafka.MessageConsumerSubscriber.MessageConsumerSubscriber
import io.github.stem.communication.kafka._
import io.github.stem.data.AlgebraCombinators.Combinators
import io.github.stem.data._
import io.github.stem.readside.ReadSideProcessing
import io.github.stem.readside.ReadSideProcessing.ReadSideProcessing
import io.github.stem.runtime.readside.CommittableJournalQuery
import io.grpc.Status
import ledger.LedgerGrpcService.{Accounts, Transactions}
import ledger.LedgerServer.emptyCombinators
import ledger.communication.grpc.ZioService.ZLedger
import ledger.communication.grpc._
import ledger.eventsourcing.events._
import ledger.messages.messages._
import scalapb.zio_grpc.{ServerMain, ServiceList}
import transactions.TransactionEntity.TransactionCommandHandler
import transactions.{TransactionEntity, TransactionId}
import zio.clock.Clock
import zio.console.Console
import zio.duration.{durationInt, Duration}
import zio.kafka.consumer.ConsumerSettings
import zio.magic._
import zio.{Has, IO, Managed, Runtime, ZEnv, ZIO, ZLayer}

sealed trait LockResponse

case object Allowed extends LockResponse

case class Denied(reason: String) extends LockResponse

object LedgerServer extends ServerMain {

  type LedgerCombinator = AlgebraCombinators.Service[Int, AccountEvent, String]
  val readSidePollingInterval: Duration = 100.millis

  private val messageHandling = LedgerInboundMessageHandling.messageHandling
    .map(
      logic =>
        KafkaMessageConsumer(
          KafkaGrpcConsumerConfiguration[LedgerId, LedgerInstructionsMessage, LedgerInstructionsMessageMessage](
            "testtopic",
            ConsumerSettings(List("0.0.0.0"))
          ),
          errorHandler,
          logic
        ): MessageConsumer[LedgerId, LedgerInstructionsMessage, String]
    )
    .toLayer

  private val accountStores = StemApp.liveRuntime[AccountId, AccountEvent]
  private val transactionStores = StemApp.liveRuntime[TransactionId, TransactionEvent]

  val emptyCombinators
    : ZLayer[Any, Nothing, Combinators[AccountState, AccountEvent, String] with Combinators[TransactionState, TransactionEvent, String]] = clientEmptyCombinator[
      AccountState,
      AccountEvent,
      String
    ] ++ clientEmptyCombinator[TransactionState, TransactionEvent, String]

  private val buildSystem: ZLayer[ZEnv, Throwable, Has[ZLedger[Any, Any]]] =
    ZLayer
      .wireSome[ZEnv, Has[ZLedger[Any, Any]]](
        StemApp.actorSettings("System"),
        accountStores,
        transactionStores,
        AccountEntity.live,
        TransactionEntity.live,
        ProcessReadSide.live,
        emptyCombinators,
        LedgerGrpcService.live,
        messageHandling,
        LedgerInboundMessageHandling.live,
        TransactionReadSideProcessor.live
      )
      .mapError(_ => new RuntimeException("Bad layer"))

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

  val live: ZLayer[Console with Clock with ReadSideProcessing with Has[CommittableJournalQuery[Long, TransactionId, TransactionEvent]] with Has[
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

  val liveHandler: ZLayer[Has[Accounts] with Has[Transactions], Throwable, Has[(LedgerId, LedgerInstructionsMessage) => IO[String, Unit]]] =
    messageHandling.toLayer

  val live: ZLayer[Console with MessageConsumerSubscriber, Nothing, Has[SubscriptionKillSwitch]] =
    ZLayer.fromManaged(
      Managed.make(
        MessageConsumerSubscriber.consumeForever
      )(_.shutdown.exitCode)
    )
}

object LedgerGrpcService {
  type Accounts = AccountId => AccountCommandHandler
  type AccountCombinator = Combinators[AccountState, AccountEvent, String]
  type TransactionCombinator = Combinators[TransactionState, TransactionEvent, String]
  type Transactions = TransactionId => TransactionCommandHandler
  type Requirements = AccountCombinator with TransactionCombinator with Has[Accounts] with Has[Transactions]

  val live: ZLayer[ZEnv with Requirements, Nothing, Has[ZLedger[Any, Any]]] =
    new ZioService.ZLedger[ZEnv with Requirements, Any] {
      def openAccount(openAccountRequest: OpenAccountRequest): ZIO[AccountCombinator with Has[Accounts], Status, OpenAccountReply] = {
        ZIO.service[Accounts].flatMap { accounts =>
          accounts(openAccountRequest.accountId).open
            .bimap(_ => Status.NOT_FOUND, _ => OpenAccountReply().withMessage("Created"))
        }
      }

      def authorizePayment(
        authorizeRequest: AuthorizeRequest
      ): ZIO[TransactionCombinator with Has[Transactions], Status, AuthorizeReply] = {
        ZIO.service[Transactions].flatMap { transactions =>
          transactions(authorizeRequest.transactionId)
            .create(authorizeRequest.from, authorizeRequest.to, authorizeRequest.amount)
            .bimap(_ => Status.NOT_FOUND, _ => AuthorizeReply().withMessage("Created"))
        }
      }
    }.toLayer

}
