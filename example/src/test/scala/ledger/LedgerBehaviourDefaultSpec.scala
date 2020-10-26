package ledger

import accounts.AccountEntity.{errorHandler, AccountCommandHandler}
import accounts.{AccountEntity, AccountId, AccountTransactionId}
import ledger.LedgerGrpcService.Accounts
import ledger.LedgerInboundMessageHandling.messageHandling
import ledger.LedgerTest._
import ledger.communication.grpc._
import ledger.eventsourcing.events._
import ledger.messages.messages.{AuthorizePaymentMessage, LedgerId, LedgerInstructionsMessage, OpenAccountMessage}
import stem.data.Versioned
import stem.runtime.akka.EventSourcedBehaviour
import stem.test.StemtityProbe
import stem.test.TestStemRuntime._
import transactions.TransactionEntity.{transactionProtocol, TransactionCommandHandler}
import transactions.{TransactionEntity, TransactionId}
import zio.duration.durationInt
import zio.test.Assertion.{equalTo, hasSameElements}
import zio.test.Eql._
import zio.test._
import zio.{ZEnv, ZIO}

object LedgerBehaviourDefaultSpec extends DefaultRunnableSpec {

  def spec = {
    val accountIdFrom = AccountId("account1")
    val accountIdTo = AccountId("account2")
    val anAccountTransactionId = AccountTransactionId("accountTransactionId")
    val transactionId = TransactionId("transactionId")

    suite("LedgerSpec")(
      suite("Ledger stemtity")(testM("receives commands, produces events and updates state") {
        (for {
          (accounts, accountsProbe) <- accountsAndProbes
          _                         <- accounts(accountIdFrom).open
          initialState              <- accountsProbe(accountIdFrom).state
          result                    <- accounts(accountIdFrom).credit(anAccountTransactionId, BigDecimal(10))
          updatedState              <- accountsProbe(accountIdFrom).state
          events                    <- accountsProbe(accountIdFrom).events
          stateFromSnapshot         <- accountsProbe(accountIdFrom).stateFromSnapshot
        } yield {
          assert(result)(equalTo(())) &&
          assert(events)(hasSameElements(List(AccountOpened(), AccountCredited(anAccountTransactionId.value, BigDecimal(10))))) &&
          assert(initialState)(equalTo(ActiveAccount(0, Set()))) &&
          assert(updatedState)(equalTo(ActiveAccount(10, Set("accountTransactionId")))) &&
          assert(stateFromSnapshot)(equalTo(Option(Versioned(2, ActiveAccount(10, Set("accountTransactionId")): AccountState))))
        }).provideLayer(env)
      }),
      suite("End to end test with memory implementations")(
        testM("End to end test using grpc service") {
          //call grpc service, check events and state, advance time, read side view, send with kafka
          (for {
            (accounts, accountsProbe) <- accountsAndProbes
            service                   <- ledgerGrpcService
            openAccountResponse       <- service.openAccount(OpenAccountRequest(accountIdFrom))
            openAccount2Response      <- service.openAccount(OpenAccountRequest(accountIdTo))

            stateInitialTo   <- accountsProbe(accountIdTo).state
            _                <- accounts(accountIdFrom).credit(anAccountTransactionId, BigDecimal(20))
            stateInitialFrom <- accountsProbe(accountIdFrom).state
            result           <- service.authorizePayment(AuthorizeRequest(transactionId, accountIdFrom, accountIdTo, BigDecimal(10)))

            transactionReadSide <- readSideClient
            _                   <- transactionReadSide.triggerReadSideAndWaitFor(triggerTimes = 2, messagesToWaitFor = 2)
            updatedState        <- accountsProbe(accountIdTo).state
          } yield {
            assert(openAccountResponse)(equalTo(OpenAccountReply("Created"))) &&
            assert(openAccount2Response)(equalTo(OpenAccountReply("Created"))) &&
            assert(stateInitialTo)(equalTo(ActiveAccount(0, Set()))) &&
            assert(stateInitialFrom)(equalTo(ActiveAccount(20, Set("accountTransactionId")))) &&
            assert(result)(equalTo(AuthorizeReply("Created"))) &&
            assert(updatedState)(equalTo(ActiveAccount(10, Set("transactionId"))))
          }).provideLayer(env)
        },
        testM("End to end test using Kafka") {
          //call grpc service, check events and state, advance time, read side view, send with kafka
          (for {
            (accounts, accountsProbe) <- accountsAndProbes
            kafka                     <- kafkaClient
            _                         <- kafka.sendAndConsume(TestMessage(LedgerId("kafkaFrom"), OpenAccountMessage(accountIdFrom)))
            _                         <- kafka.sendAndConsume(TestMessage(LedgerId("kafkaTo"), OpenAccountMessage(accountIdTo)))
            messageArrived            <- kafka.hasMessagesArrivedInTimeWindow(1.millis)
            stateInitialTo            <- accountsProbe(accountIdTo).state
            _                         <- accounts(accountIdFrom).credit(anAccountTransactionId, BigDecimal(20))
            stateInitialFrom          <- accountsProbe(accountIdFrom).state
            _                         <- kafka.sendAndConsume(TestMessage(LedgerId("kafkaKey"), AuthorizePaymentMessage(transactionId, accountIdFrom, accountIdTo, BigDecimal(10))))

            transactionReadSide <- readSideClient
            _                   <- transactionReadSide.triggerReadSideAndWaitFor(triggerTimes = 2, messagesToWaitFor = 2)
            updatedState        <- accountsProbe(accountIdTo).state
          } yield {
            assert(messageArrived)(equalTo(false)) &&
            assert(stateInitialTo)(equalTo(ActiveAccount(0, Set()))) &&
            assert(stateInitialFrom)(equalTo(ActiveAccount(20, Set("accountTransactionId")))) &&
            assert(updatedState)(equalTo(ActiveAccount(10, Set("transactionId"))))
          }).provideLayer(env)
        }
      )
    )
  }

  // real from stubbed are these modules: readSideProcessing, memoryStores and testStem/stem
  // helper method to retrieve stemtity, probe and grpc

  private def env = {
    val transactionsAndAccounts = stemtityAndReadSideLayer[TransactionId, TransactionCommandHandler, TransactionState, TransactionEvent, String](
        TransactionEntity.tagging,
        EventSourcedBehaviour(new TransactionCommandHandler(), TransactionEntity.eventHandlerLogic, TransactionEntity.errorHandler),
        snapshotInterval = 2
      ) and stemtityAndReadSideLayer[AccountId, AccountCommandHandler, AccountState, AccountEvent, String](
        AccountEntity.tagging,
        EventSourcedBehaviour(new AccountCommandHandler(), AccountEntity.eventHandlerLogic, AccountEntity.errorHandler),
        snapshotInterval = 2
      )
    val processReadSide = transactionsAndAccounts to ProcessReadSide.live
    val grpc = transactionsAndAccounts and processReadSide to LedgerGrpcService.live
    val readSide = transactionsAndAccounts and (processReadSide to TransactionReadSideProcessor.readsideParams.toLayer) to TestReadSideProcessor
        .memory[TransactionId, TransactionEvent, Long, String](errorHandler = errorHandler)
    val consumer = (transactionsAndAccounts to messageHandling.toLayer) to TestKafkaMessageConsumer
        .memory[LedgerId, LedgerInstructionsMessage, String]

    transactionsAndAccounts ++ processReadSide ++ grpc ++ readSide ++ consumer
  }
}

object LedgerTest {
  val accountsAndProbes =
    ZIO.services[Accounts, StemtityProbe.Service[AccountId, AccountState, AccountEvent]]
  val ledgerGrpcService = ZIO.service[ZioService.ZLedger[ZEnv, Any]]
  val kafkaClient = ZIO.service[TestKafka[LedgerId, LedgerInstructionsMessage, String]]
  val readSideClient = ZIO.service[TestReadSideProcessor.TestReadSideProcessor[String]]
}
