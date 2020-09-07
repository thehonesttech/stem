package ledger

import ledger.Converters.toLedgerBigDecimal
import ledger.LedgerEntity.LedgerCommandHandler
import ledger.communication.grpc.service.{LockReply, LockRequest, ZioService}
import ledger.eventsourcing.events.events.{AmountLocked, LedgerEvent}
import stem.runtime.akka.EventSourcedBehaviour
import stem.test.TestStemRuntime._
import stem.test.{StemOps, StemtityProbe}
import zio.duration.durationInt
import zio.test.Assertion.{equalTo, hasSameElements}
import zio.test._
import zio.test.environment.{TestClock, TestConsole}
import zio.{ZEnv, ZIO}
import Eql._

object LedgerBehaviourDefaultSpec extends DefaultRunnableSpec with StemOps {

  def spec = suite("LedgerSpec")(
    suite("Ledger stemtity with memory stemtity")(testM("receives commands, produces events and update state") {
      val key = "key"
      (for {
        ledgers         <- ledgerStemtity
        probe           <- ledgerProbe
        result          <- ledgers(key).lock(BigDecimal(10), "test1")
        events          <- probe(key).events
        stateInitial    <- probe(key).state
        _               <- ledgers(key).lock(BigDecimal(12), "test2")
        events2         <- probe(key).events
        stateSecondCall <- probe(key).state
      } yield {
        assert(result)(equalTo(Allowed)) &&
        assert(events)(hasSameElements(List(AmountLocked(toLedgerBigDecimal(BigDecimal(10)), "test1")))) &&
        assert(stateInitial)(equalTo(1)) &&
        assert(events2)(
          hasSameElements(List(AmountLocked(toLedgerBigDecimal(BigDecimal(10)), "test1"), AmountLocked(toLedgerBigDecimal(BigDecimal(12)), "test2")))
        ) &&
        assert(stateSecondCall)(equalTo(2))
      }).provideLayer( (testComponentsLayer >>> LedgerGrpcService.live) ++ testComponentsLayer)
    }),


    suite("End to end test with memory implementations")(
      testM("End to end test") {
        import Converters.Ops._
        //call grpc service, check events and state, advance time, read side view, send with kafka
        (for {
          probe        <- ledgerProbe
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
  private val ledgerStemtity = ZIO.service[String => LedgerCommandHandler]
  private val ledgerProbe = ZIO.service[StemtityProbe.Service[String, Int, LedgerEvent]]
  private val ledgerGrpcService = ZIO.service[ZioService.ZLedger[ZEnv, Any]]

  private def testComponentsLayer = stemtityAndReadSideLayer[String, LedgerCommandHandler, Int, LedgerEvent, String](
    LedgerEntity.tagging,
    EventSourcedBehaviour(new LedgerCommandHandler(), LedgerEntity.eventHandlerLogic, LedgerEntity.errorHandler)
  )

  private def env = {
    val testCompLayer = testComponentsLayer
     (testCompLayer >>> LedgerGrpcService.live) ++
      testCompLayer ++
      (testCompLayer >>> LedgerReadSideProcessor.live)
  }
}
