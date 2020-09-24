package ledger

import ledger.Converters.toLedgerBigDecimal
import ledger.LedgerEntity.LedgerCommandHandler
import ledger.LedgerGrpcService.Ledgers
import ledger.LedgerInboundMessageHandling.messageHandling
import ledger.LedgerTest._
import ledger.communication.grpc.service.{LockReply, LockRequest, ZioService}
import ledger.eventsourcing.events.events.{AmountLocked, LedgerEvent}
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
          (ledgers, probe)            <- ledgerStemtityAndProbe
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
          assert(events)(hasSameElements(List(AmountLocked(toLedgerBigDecimal(BigDecimal(10)), "test1")))) &&
          assert(initialState)(equalTo(1)) &&
          assert(stateFromSnapshot)(isNone) &&
          assert(events2)(
            hasSameElements(List(AmountLocked(toLedgerBigDecimal(BigDecimal(10)), "test1"), AmountLocked(toLedgerBigDecimal(BigDecimal(12)), "test2")))
          ) &&
          assert(updatedState)(equalTo(2)) &&
          assert(initialStateForSecondEntity)(equalTo(1)) &&
          assert(stateAfter2Events)(isSome(equalTo((Versioned(2, updatedState)))))
        }).provideLayer(env)
      }),
      suite("End to end test with memory implementations")(
        testM("End to end test using grpc service") {
          import Converters.Ops._
          val key = "key4"
          //call grpc service, check events and state, advance time, read side view, send with kafka
          (for {
            (_, probe)   <- ledgerStemtityAndProbe
            service      <- ledgerGrpcService
            lockReply    <- service.lock(LockRequest(key, "accountId1", BigDecimal(10), "idempotency1"))
            stateInitial <- probe(key).state
            readSide     <- readSideClient
            _            <- readSide.triggerReadSideAndWaitFor(1)
            console      <- TestConsole.output
          } yield {
            assert(lockReply)(equalTo(LockReply("Allowed"))) &&
            assert(stateInitial)(equalTo(1)) &&
            assert(console)(equalTo(Vector("Allowed\n", "Arrived key4\n")))
          }).provideLayer(env)
        },
        testM("End to end test using Kafka") {
          import Converters.Ops._
          //call grpc service, check events and state, advance time, read side view, send with kafka
          val accountId = "accountId1"
          (for {
            (_, probe)     <- ledgerStemtityAndProbe
            kafka          <- kafkaClient
            _              <- kafka.sendAndConsume(TestMessage(LedgerId("kafkaKey"), Authorization(accountId, BigDecimal(10), "idempotency1")))
            messageArrived <- kafka.hasMessagesArrivedInTimeWindow(1.millis)
            stateInitial   <- probe(accountId).state
            readSide       <- readSideClient
            _              <- readSide.triggerReadSideAndWaitFor(1)
            console        <- TestConsole.output
          } yield {
            assert(messageArrived)(equalTo(false)) &&
            assert(stateInitial)(equalTo(1)) &&
            assert(console)(equalTo(Vector("Arrived accountId1\n")))
          }).provideLayer(env)
        }
      )
    )

  // real from stubbed are these modules: readSideProcessing, memoryStores and testStem/stem
  // helper method to retrieve stemtity, probe and grpc

  private val testMessageConsumer: ZLayer[Has[Ledgers], Throwable, Has[MessageConsumer[LedgerId, LedgerInstructionsMessage]] with Has[
    TestStemRuntime.TestKafka[LedgerId, LedgerInstructionsMessage]
  ]] = messageHandling.toLayer >>> TestKafkaMessageConsumer
      .memory[LedgerId, LedgerInstructionsMessage]

  private def env = {
    (stemtityAndReadSideLayer[String, LedgerCommandHandler, Int, LedgerEvent, String](
      LedgerEntity.tagging,
      EventSourcedBehaviour(new LedgerCommandHandler(), LedgerEntity.eventHandlerLogic, LedgerEntity.errorHandler),
      snapshotInterval = 2
    ) >+> LedgerReadSideProcessor.readSideParams.toLayer) >+> (LedgerGrpcService.live ++ TestReadSideProcessor
      .memory[String, LedgerEvent, Long] ++ testMessageConsumer)
  }
}

object LedgerTest {
  val ledgerStemtityAndProbe = ZIO.services[String => LedgerCommandHandler, StemtityProbe.Service[String, Int, LedgerEvent]]
  val ledgerGrpcService = ZIO.service[ZioService.ZLedger[ZEnv, Any]]
  val kafkaClient = ZIO.service[TestKafka[LedgerId, LedgerInstructionsMessage]]
  val readSideClient = ZIO.service[TestReadSideProcessor.TestReadSideProcessor]
}
