package ledger

import ledger.Converters.toLedgerBigDecimal
import ledger.LedgerEntity.LedgerCommandHandler
import ledger.communication.grpc.service.{LockReply, LockRequest, ZioService}
import ledger.eventsourcing.events.events.{AmountLocked, LedgerEvent}
import stem.StemApp
import stem.readside.ReadSideProcessing
import stem.runtime.akka.EventSourcedBehaviour
import stem.runtime.readside.JournalStores.memoryJournalAndQueryStoreLayer
import stem.test.TestStemRuntime._
import stem.test.{StemOps, StemtityProbe}
import zio.clock.Clock
import zio.console.Console
import zio.duration.durationInt
import zio.test.Assertion.{equalTo, hasSameElements}
import zio.test._
import zio.test.environment.{TestClock, TestConsole}
import zio.{ZEnv, ZIO, ZLayer}

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
      }).provideLayer(zio.test.environment.testEnvironment ++ ledgerCombinator ++ stemtityLayer ++ probeLayer)
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
          _            <- TestClock.adjust(100.millis)
//          sleeps       <- TestClock.sleeps
//          eventsFromReadSide <- probe.eventsFromReadSide(LedgerEntity.tagging.tag)
          console <- TestConsole.output
//          _       <- shutdownReadSide
//          _ = println(sleeps)
        } yield {
          assert(lockReply)(equalTo(LockReply("Allowed"))) &&
          assert(stateInitial)(equalTo(1)) &&
//          assert(eventsFromReadSide)(equalTo(List(AmountLocked(toLedgerBigDecimal(BigDecimal(10)), "idempotency1")))) &&
          assert(console)(equalTo(Vector("Allowed\n")))
        }).provideLayer(zio.test.environment.testEnvironment ++ TestConsole.silent ++ TestClock.any ++ probeLayer ++ grpcServiceLayer ++ readSideProcessorLayer)
      }
    )
  )

  private val ledgerProbe = ZIO.service[StemtityProbe.Service[String, Int, LedgerEvent]]
  private val ledgerStemtity = ZIO.service[String => LedgerCommandHandler]
  private val shutdownReadSide = ZIO.service[ReadSideProcessing.KillSwitch].flatMap(_.shutdown)

  private val ledgerGrpcService = ZIO.service[ZioService.ZLedger[ZEnv, Any]]
  private val (memoryEventJournalLayer, committableJournalQueryStore) = memoryJournalAndQueryStoreLayer[String, LedgerEvent]
  private val readSideProcessorLayer = Console.any ++ Clock.any ++ committableJournalQueryStore ++ ReadSideProcessing.memory >>> ReadSideProcessor.live

//  private val ledgerCombinator = testLayer[Int, LedgerEvent, String]
  private val ledgerCombinator = StemApp.stubCombinator[Int, LedgerEvent, String]
  private val stemtityLayer = (memoryEventJournalLayer >>> stemtity[String, LedgerCommandHandler, Int, LedgerEvent, String](
    LedgerEntity.tagging,
    EventSourcedBehaviour(new LedgerCommandHandler(), LedgerEntity.eventHandlerLogic, LedgerEntity.errorHandler)
  ).toLayer)

  private val probeLayer = memoryEventJournalLayer ++ ZLayer.succeed(
    LedgerEntity.eventHandlerLogic
  ) >>> StemtityProbe.live[String, Int, LedgerEvent]
  private val grpcServiceLayer = stemtityLayer >>> LedgerGrpcService.live
}
