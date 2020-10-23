package ledger

import accounts.{AccountEntity, AccountId, AccountTransactionId}
import accounts.AccountEntity.{errorHandler, AccountCommandHandler}
import ledger.LedgerGrpcService.{Accounts, Transactions}
import ledger.LedgerInboundMessageHandling.messageHandling
import ledger.LedgerTest._
import ledger.communication.grpc.{AuthorizeReply, AuthorizeRequest, OpenAccountReply, OpenAccountRequest, ZioService}
import ledger.eventsourcing.events.{AccountCredited, AccountEvent, AccountOpened, AccountState, ActiveAccount, EmptyAccount, TransactionEvent, TransactionState}
import ledger.messages.messages.{AuthorizePaymentMessage, LedgerId, LedgerInstructionsMessage}
import stem.communication.kafka.MessageConsumer
import stem.data.Versioned
import stem.runtime.akka.EventSourcedBehaviour
import stem.test.TestStemRuntime._
import stem.test.{StemtityProbe, TestStemRuntime}
import transactions.TransactionEntity.{transactionProtocol, TransactionCommandHandler}
import transactions.{TransactionEntity, TransactionId}
import zio.duration.durationInt
import zio.test.Assertion.{equalTo, hasSameElements, isNone, isSome, _}
import zio.test.Eql._
import zio.test._
import zio.test.environment.TestConsole
import zio.{Has, ZEnv, ZIO, ZLayer}

object LedgerBehaviourDefaultSpec extends DefaultRunnableSpec {

  def spec =
    suite("LedgerSpec")(
      suite("Ledger stemtity")(testM("receives commands, produces events and updates state") {
        val (accountId, transactionId) = (AccountId("account1"), AccountTransactionId("transactionId"))
        //val (accountId2, transactionId2) = (AccountId("accountId2"), TransactionId("transactionId2"))
        (for {
          (accounts, accountsProbe) <- accountsAndProbes
          _                         <- accounts(accountId).open
          initialState              <- accountsProbe(accountId).state
          result                    <- accounts(accountId).credit(transactionId, BigDecimal(10))
          updatedState              <- accountsProbe(accountId).state
          events                    <- accountsProbe(accountId).events
          stateFromSnapshot         <- accountsProbe(accountId).stateFromSnapshot
        } yield {
          assert(result)(equalTo(())) &&
          assert(events)(hasSameElements(List(AccountOpened(), AccountCredited(transactionId.value, BigDecimal(10))))) &&
          assert(initialState)(equalTo(ActiveAccount(0, Set()))) &&
          assert(updatedState)(equalTo(ActiveAccount(10, Set("transactionId")))) &&
          assert(stateFromSnapshot)(equalTo(Option(Versioned(2, ActiveAccount(10, Set("transactionId")): AccountState))))
        }).provideLayer(env)
      }),
      suite("End to end test with memory implementations")(
        testM("End to end test using grpc service") {
          val accountIdFrom = AccountId("account1")
          val accountIdTo = AccountId("account2")
          val anAccountTransactionId = AccountTransactionId("creditTransactionId")
          val transactionId = TransactionId("transactionId")
          //call grpc service, check events and state, advance time, read side view, send with kafka
          (for {
            // (_, account, transaction) <- ledgerLogicAndProbes
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

            // readSide                  <- readSideClient
            // _                         <- readSide.triggerReadSideAndWaitFor(1)
            //console                   <- TestConsole.output
          } yield {
            assert(openAccountResponse)(equalTo(OpenAccountReply("Created"))) &&
            assert(openAccount2Response)(equalTo(OpenAccountReply("Created"))) &&
            assert(stateInitialTo)(equalTo(ActiveAccount(0, Set()))) &&
            assert(stateInitialFrom)(equalTo(ActiveAccount(20, Set("creditTransactionId")))) &&
            assert(result)(equalTo(AuthorizeReply("Created"))) &&
            assert(updatedState)(equalTo(ActiveAccount(10, Set("transactionId"))))
          }).provideLayer(env)
        } /*
        testM("End to end test using Kafka") {
          //call grpc service, check events and state, advance time, read side view, send with kafka
          val accountId = "accountId1"
          (for {
            (_, probe)     <- ledgerLogicAndProbes
            kafka          <- kafkaClient
            _              <- kafka.sendAndConsume(TestMessage(LedgerId("kafkaKey"), Authorization(accountId)))
            messageArrived <- kafka.hasMessagesArrivedInTimeWindow(1.millis)
            stateInitial   <- probe(accountId).state
            readSide       <- readSideClient
            _              <- readSide.triggerReadSideAndWaitFor(1)
            console        <- TestConsole.output
          } yield {
            assert(messageArrived)(equalTo(false)) &&
            assert(stateInitial)(equalTo(EmptyAccount())) &&
            assert(console)(equalTo(Vector("Arrived accountId1\n")))
          }).provideLayer(env)
        }*/
      )
    )

  // real from stubbed are these modules: readSideProcessing, memoryStores and testStem/stem
  // helper method to retrieve stemtity, probe and grpc

  private val testMessageConsumer
    : ZLayer[Has[Accounts] with Has[Transactions], Throwable, Has[MessageConsumer[LedgerId, LedgerInstructionsMessage, String]] with Has[
      TestStemRuntime.TestKafka[LedgerId, LedgerInstructionsMessage, String]
    ]] = messageHandling.toLayer >>> TestKafkaMessageConsumer
      .memory[LedgerId, LedgerInstructionsMessage, String]

  private def env = {
    val transactions = stemtityAndReadSideLayer[TransactionId, TransactionCommandHandler, TransactionState, TransactionEvent, String](
      TransactionEntity.tagging,
      EventSourcedBehaviour(new TransactionCommandHandler(), TransactionEntity.eventHandlerLogic, TransactionEntity.errorHandler),
      snapshotInterval = 2
    )
    val accounts = stemtityAndReadSideLayer[AccountId, AccountCommandHandler, AccountState, AccountEvent, String](
      AccountEntity.tagging,
      EventSourcedBehaviour(new AccountCommandHandler(), AccountEntity.eventHandlerLogic, AccountEntity.errorHandler),
      snapshotInterval = 2
    )

    val transactionsAndAccounts = transactions and accounts
    val processReadSide = transactionsAndAccounts to ProcessReadSide.live
    val grpc = transactionsAndAccounts and processReadSide to LedgerGrpcService.live
    val readSide = transactions and (processReadSide to TransactionReadSideProcessor.readsideParams.toLayer) to TestReadSideProcessor
        .memory[TransactionId, TransactionEvent, Long, String](errorHandler = errorHandler)
    val consumer = transactionsAndAccounts and processReadSide to testMessageConsumer

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
