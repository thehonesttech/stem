package ledger

import ledger.LedgerEntity.LedgerCommandHandler
import ledger.eventsourcing.events.events.LedgerEvent
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import stem.test.StemOps
import stem.test.TestStemRuntime._

class LedgerBehaviourSpec extends AnyFreeSpec with Matchers with TypeCheckedTripleEquals with StemOps {

  "In a Stem Ledger behaviour" - {
    "should be easy to test the entity not using actorsystem" in {
      // when a command happens, events are triggered and state updated
      val ledgers = testStemtity[LedgerCommandHandler, String, LedgerEvent, Int, String](new LedgerCommandHandler(), LedgerEntity.eventHandlerLogic)

      ledgers("key").lock(BigDecimal(10), "test1").runSync should ===(Allowed)

      // events are generated, state is retrieved
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
