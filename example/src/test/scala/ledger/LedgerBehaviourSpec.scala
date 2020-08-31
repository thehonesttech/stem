package ledger

import java.time.Instant

import ledger.LedgerEntity.LedgerCommandHandler
import ledger.eventsourcing.events.events.LedgerEvent
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import stem.data.EventTag
import stem.data.Tagging.Const
import stem.runtime.akka.EventSourcedBehaviour
import stem.test.StemOps
import stem.test.TestStemRuntime._
import zio.duration._
import zio.test.Assertion.equalTo
import zio.test._
import zio.test.AssertionM._
import zio.test.environment.TestClock

class LedgerBehaviourSpec extends AnyFreeSpec with Matchers with TypeCheckedTripleEquals with StemOps {

  "In a Stem Ledger behaviour" - {
    val ledgerBehaviour =
      EventSourcedBehaviour(new LedgerCommandHandler(), LedgerEntity.eventHandlerLogic, LedgerEntity.errorHandler)
    "should be easy to test the entity not using actorsystem" in {
      // when a command happens, events are triggered and state updated
      val (result, events, stateInitial, stateAfter, events2) = (for {
        ledgerWithProbe <- memoryStemtity[String, LedgerCommandHandler, Int, LedgerEvent, String](Const(EventTag("testKey")), ledgerBehaviour)
        LedgerWithProbe(ledgers, probe) = ledgerWithProbe
        result <- ledgers("key").lock(BigDecimal(10), "test1")
        _      <- TestClock.adjust(1.seconds)
        events <- probe("key").getEvents
        stateInitial <- probe("key").getState
        _ <- ledgers("key").lock(BigDecimal(10), "test1")
        events2 <- probe("key").getEvents
        stateAfter <- probe("key").getState
      } yield (result,events, stateInitial, stateAfter, events2)).provideLayer(testLayer[Int, LedgerEvent, String]).runSync

      result should ===(Allowed)
      events should have size 1
      stateInitial should ===(1)
      stateAfter should ===(2)
      events2 should have size 2

    }

    "should be easy to test the entity using actorsystem" in {
      fail()
    }

    "should be easy to test end to end with actorsystem" in {
      // grpc, command, event, state, read view, to kafka
      //multiple tasks together

      fail()
    }

    "should be easy to test end to end without actorsystem" in {
      // grpc, command, event, state, read view, to kafka
      fail()
    }

    "should be easy to test inbound kafka interaction" in {
      fail()
    }

    "should be easy to test grpc interaction" in {
      fail()
    }

  }

}
