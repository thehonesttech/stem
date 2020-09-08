package ledger

import ledger.Converters.toLedgerBigDecimal
import ledger.LedgerEntity.LedgerCommandHandler
import ledger.communication.grpc.service.{LockReply, LockRequest, ZioService}
import ledger.eventsourcing.events.events.{AmountLocked, LedgerEvent}
import stem.data.Versioned
import stem.runtime.akka.EventSourcedBehaviour
import stem.test.StemtityProbe
import stem.test.TestStemRuntime._
import zio.duration.durationInt
import zio.test.Assertion.{equalTo, hasSameElements}
import zio.test.Eql._
import zio.test._
import zio.test.environment.{TestClock, TestConsole}
import zio.{ZEnv, ZIO}

object LedgerBehaviourDefaultSpec extends DefaultRunnableSpec {

  def spec =
    suite("LedgerSpec")(
      suite("Ledger stemtity with memory stemtity")(testM("receives commands, produces events and updates state") {
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
          assert(stateFromSnapshot)(equalTo(None)) &&
          assert(events2)(
            hasSameElements(List(AmountLocked(toLedgerBigDecimal(BigDecimal(10)), "test1"), AmountLocked(toLedgerBigDecimal(BigDecimal(12)), "test2")))
          ) &&
          assert(updatedState)(equalTo(2)) &&
          assert(initialStateForSecondEntity)(equalTo(1)) &&
          assert(stateAfter2Events)(equalTo(Some(Versioned(2, updatedState))))
        }).provideLayer(testComponentsLayer)
      }),
      suite("End to end test with memory implementations")(
        testM("End to end test") {
          import Converters.Ops._
          //call grpc service, check events and state, advance time, read side view, send with kafka
          (for {
            (_, probe)   <- ledgerStemtityAndProbe
            service      <- ledgerGrpcService
            lockReply    <- service.lock(LockRequest("key", "accountId1", BigDecimal(10), "idempotency1"))
            stateInitial <- probe("key").state
            _            <- TestClock.adjust(200.millis) // read side is executing code (100 millis is the polling interval)
            console      <- TestConsole.output
          } yield {
            assert(lockReply)(equalTo(LockReply("Allowed"))) &&
            assert(stateInitial)(equalTo(1)) &&
            assert(console)(equalTo(Vector("Allowed\n", "Arrived key\n")))
          }).provideLayer(env)
        }
      )
    ).provideCustomLayerShared(TestConsole.silent)

  // helper method to retrieve stemtity, probe and grpc
  private val ledgerStemtityAndProbe = ZIO.services[String => LedgerCommandHandler, StemtityProbe.Service[String, Int, LedgerEvent]]
  private val ledgerGrpcService = ZIO.service[ZioService.ZLedger[ZEnv, Any]]

  private def testComponentsLayer = stemtityAndReadSideLayer[String, LedgerCommandHandler, Int, LedgerEvent, String](
    LedgerEntity.tagging,
    EventSourcedBehaviour(new LedgerCommandHandler(), LedgerEntity.eventHandlerLogic, LedgerEntity.errorHandler),
    snapshotInterval = 2
  )

  private def env = {
    val testCompLayer = testComponentsLayer
    testCompLayer >+> (LedgerGrpcService.live ++ LedgerReadSideProcessor.live)
  }
}
