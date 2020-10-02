package ledger

import accounts.AccountEntity
import accounts.AccountEntity.AccountCommandHandler
import .toGrpcBigDecimal
import ledger.LedgerGrpcService.Accounts
import ledger.LedgerInboundMessageHandling.messageHandling
import ledger.LedgerTest._
import ledger.communication.grpc.service.{LockReply, LockRequest, ZioService}
import ledger.eventsourcing.events.{AccountEvent, AccountState, AmountLocked, EmptyAccount}
import ledger.messages.messages.{Authorization, LedgerId, LedgerInstructionsMessage}
import stem.communication.kafka.MessageConsumer
import stem.data.Versioned
import stem.runtime.akka.EventSourcedBehaviour
import stem.test.TestStemRuntime._
import stem.test.{StemtityProbe, TestStemRuntime}
import zio.duration.durationInt
import zio.test.Assertion.{equalTo, hasSameElements, isNone, isSome}
import zio.test.Eql._
import zio.test._
import zio.test.environment.TestConsole
import zio.{Has, ZEnv, ZIO, ZLayer}

object LedgerBehaviourDefaultSpec extends DefaultRunnableSpec {

  def spec =
    suite("LedgerSpec")(
      suite("Ledger stemtity")(testM("receives commands, produces events and updates state") {
        val key = "key"
        val keyOther = "key2"
        (for {
          (ledgers, probe)            <- accountStemtityAndProbe
          result                      <- ledgers(key).lock(BigDecimal(10), "test1")
          events                      <- probe(key).events
          initialState                <- probe(key).state
          stateFromSnapshot           <- probe(key).stateFromSnapshot
          _                           <- ledgers(key).lock(BigDecimal(12), "test2")
          events2                     <- probe(key).events
          updatedState                <- probe(key).state
          stateAfter2Events           <- probe(key).stateFromSnapshot
          _                           <- ledgers(keyOther).lock(BigDecimal(5), "test4")
          initialStateForSecondEntity <- probe(keyOther).state
        } yield {
          assert(result)(equalTo(Allowed)) &&
          assert(events)(hasSameElements(List(AmountLocked(toGrpcBigDecimal(BigDecimal(10)), "test1")))) &&
          assert(initialState)(equalTo(EmptyAccount())) &&
          assert(stateFromSnapshot)(isNone) &&
          assert(events2)(
            hasSameElements(List(AmountLocked(toGrpcBigDecimal(BigDecimal(10)), "test1"), AmountLocked(toGrpcBigDecimal(BigDecimal(12)), "test2")))
          ) &&
          assert(updatedState)(equalTo(EmptyAccount())) &&
          assert(initialStateForSecondEntity)(equalTo(EmptyAccount())) &&
          assert(stateAfter2Events)(isSome(equalTo((Versioned(2, updatedState)))))
        }).provideLayer(env)
      }),
      suite("End to end test with memory implementations")(
        testM("End to end test using grpc service") {
          import .Ops._
          val key = "key4"
          //call grpc service, check events and state, advance time, read side view, send with kafka
          (for {
            (_, probe)   <- accountStemtityAndProbe
            service      <- ledgerGrpcService
            lockReply    <- service.lock(LockRequest(key, "accountId1", BigDecimal(10), "idempotency1"))
            stateInitial <- probe(key).state
            readSide     <- readSideClient
            _            <- readSide.triggerReadSideAndWaitFor(1)
            console      <- TestConsole.output
          } yield {
            assert(lockReply)(equalTo(LockReply("Allowed"))) &&
            assert(stateInitial)(equalTo(EmptyAccount())) &&
            assert(console)(equalTo(Vector("Allowed\n", "Arrived key4\n")))
          }).provideLayer(env)
        },
        testM("End to end test using Kafka") {
          import .Ops._
          //call grpc service, check events and state, advance time, read side view, send with kafka
          val accountId = "accountId1"
          (for {
            (_, probe)     <- accountStemtityAndProbe
            kafka          <- kafkaClient
            _              <- kafka.sendAndConsume(TestMessage(LedgerId("kafkaKey"), Authorization(accountId, BigDecimal(10), "idempotency1")))
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
        }
      )
    )

  // real from stubbed are these modules: readSideProcessing, memoryStores and testStem/stem
  // helper method to retrieve stemtity, probe and grpc

  private val testMessageConsumer: ZLayer[Has[Accounts], Throwable, Has[MessageConsumer[LedgerId, LedgerInstructionsMessage]] with Has[
    TestStemRuntime.TestKafka[LedgerId, LedgerInstructionsMessage]
  ]] = messageHandling.toLayer >>> TestKafkaMessageConsumer
      .memory[LedgerId, LedgerInstructionsMessage]

  private def env = {
    (stemtityAndReadSideLayer[String, AccountCommandHandler, AccountState, AccountEvent, String](
      AccountEntity.tagging,
      EventSourcedBehaviour(new AccountCommandHandler(), AccountEntity.eventHandlerLogic, AccountEntity.errorHandler),
      snapshotInterval = 2
    ) >+> LedgerReadSideProcessor.readSideParams.toLayer) >+> (LedgerGrpcService.live ++ TestReadSideProcessor
      .memory[String, AccountEvent, Long] ++ testMessageConsumer)
  }
}

object LedgerTest {
  val accountStemtityAndProbe = ZIO.services[String => AccountCommandHandler, StemtityProbe.Service[String, AccountState, AccountEvent]]
  val ledgerGrpcService = ZIO.service[ZioService.ZLedger[ZEnv, Any]]
  val kafkaClient = ZIO.service[TestKafka[LedgerId, LedgerInstructionsMessage]]
  val readSideClient = ZIO.service[TestReadSideProcessor.TestReadSideProcessor]
}
