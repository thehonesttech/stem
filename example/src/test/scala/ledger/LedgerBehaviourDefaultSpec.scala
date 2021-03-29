package ledger

import accounts.AccountEntity.{errorHandler, AccountCommandHandler}
import accounts.{AccountEntity, AccountId, AccountTransactionId}
import io.github.stem.data.Versioned
import io.github.stem.readside.ReadSideProcessing
import io.github.stem.runtime.akka.EventSourcedBehaviour
import io.github.stem.test.StemtityProbe
import io.github.stem.test.TestStemRuntime.TestReadSideProcessor.TestReadSideProcessor
import io.github.stem.test.TestStemRuntime._
import ledger.LedgerServer.Accounts
import ledger.LedgerTest._
import ledger.communication.grpc.ZioService.ZLedger
import ledger.communication.grpc._
import ledger.eventsourcing.events._
import ledger.messages.messages.{AuthorizePaymentMessage, LedgerId, LedgerInstructionsMessage, OpenAccountMessage}
import transactions.TransactionEntity.{transactionProtocol, TransactionCommandHandler}
import transactions.{TransactionEntity, TransactionId}
import zio.duration.durationInt
import zio.magic._
import zio.test.Assertion.{equalTo, hasSameElements}
import zio.test.Eql._
import zio.test._
import zio.test.environment.TestEnvironment
import zio.{Has, ZIO, ZLayer}

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
        }).injectSome[TestEnvironment](
          testStemtityAndStores[AccountId, AccountCommandHandler, AccountState, AccountEvent, String](
            AccountEntity.tagging,
            EventSourcedBehaviour(new AccountCommandHandler(), AccountEntity.eventHandlerLogic, AccountEntity.errorHandler),
            snapshotInterval = 2
          )
        )
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
          }).injectSome[TestEnvironment](
            env
          )
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
          }).injectSome[TestEnvironment](env)
        }
      )
    )
  }

  // real from stubbed are these modules: readSideProcessing, memoryStores and testStem/io.github.stem
  // helper method to retrieve stemtity, probe and grpc
  import zio.magic._
  type TestAccounts = TestStemtity[AccountId, AccountCommandHandler, AccountState, AccountEvent, String]
  type TestTransactions = TestStemtity[TransactionId, TransactionCommandHandler, TransactionState, TransactionEvent, String]
  type TestKafkaConsumer = TestConsumer[LedgerId, LedgerInstructionsMessage, String]

  private val accountLayers = testStemtityAndStores[AccountId, AccountCommandHandler, AccountState, AccountEvent, String](
    AccountEntity.tagging,
    EventSourcedBehaviour(new AccountCommandHandler(), AccountEntity.eventHandlerLogic, AccountEntity.errorHandler),
    snapshotInterval = 2
  )

  private val transactionLayers = testStemtityAndStores[TransactionId, TransactionCommandHandler, TransactionState, TransactionEvent, String](
    TransactionEntity.tagging,
    EventSourcedBehaviour(new TransactionCommandHandler(), TransactionEntity.eventHandlerLogic, TransactionEntity.errorHandler),
    snapshotInterval = 2
  )

  private val kafka = TestKafkaMessageConsumer
    .memory[LedgerId, LedgerInstructionsMessage, String]
  private val testReadSideProcessor = TestReadSideProcessor
    .memory[TransactionId, TransactionEvent, Long, String](errorHandler = errorHandler)
  private val env
    : ZLayer[TestEnvironment, Throwable, TestTransactions with TestAccounts with Has[ZLedger[Any, Any]] with Has[TestReadSideProcessor[String]] with TestKafkaConsumer] =
    ZLayer
      .wireSome[TestEnvironment, TestTransactions with TestAccounts with Has[
        ZLedger[Any, Any]
      ] with Has[TestReadSideProcessor[String]] with TestConsumer[LedgerId, LedgerInstructionsMessage, String]](
        accountLayers,
        transactionLayers,
        ProcessReadSide.live,
        LedgerGrpcService.live,
        TransactionReadSideProcessor.readsideParams.toLayer,
        testReadSideProcessor,
        LedgerInboundMessageHandling.liveHandler,
        ReadSideProcessing.memory,
        kafka
      )
}

object LedgerTest {
  val accountsAndProbes =
    ZIO.services[Accounts, StemtityProbe.Service[AccountId, AccountState, AccountEvent]]
  val ledgerGrpcService = ZIO.service[ZioService.ZLedger[Any, Any]]
  val kafkaClient = ZIO.service[TestKafkaMessageConsumer[LedgerId, LedgerInstructionsMessage, String]]
  val readSideClient = ZIO.service[TestReadSideProcessor.TestReadSideProcessor[String]]
}
