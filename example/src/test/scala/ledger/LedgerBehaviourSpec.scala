package ledger

import java.time.Instant

import ledger.LedgerEntity.LedgerCommandHandler
import ledger.eventsourcing.events.events.LedgerEvent
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import stem.data.{AlgebraCombinators, EventTag}
import stem.data.Tagging.Const
import stem.runtime.KeyedAlgebraCombinators
import stem.runtime.akka.EventSourcedBehaviour
import stem.test.StemOps
import stem.test.TestStemRuntime._
import zio.Task
import zio.test._
import zio.test.environment.{TestClock, testEnvironment}

class LedgerBehaviourSpec extends AnyFreeSpec with Matchers with TypeCheckedTripleEquals with StemOps {

  "In a Stem Ledger behaviour" - {
    val ledgerBehaviour =
      EventSourcedBehaviour(new LedgerCommandHandler(), LedgerEntity.eventHandlerLogic, LedgerEntity.errorHandler)
    "should be easy to test the entity not using actorsystem" in {
      // when a command happens, events are triggered and state updated
        (for {
          ledgers <- memoryStemtity[String, LedgerCommandHandler, Int, LedgerEvent, String](Const(EventTag("testKey")), ledgerBehaviour).mapError(_ => "Error")
          _ = println("C " +Instant.now())
          result <- ledgers("key").lock(BigDecimal(10), "test1")
          result2 <- ledgers("key2").lock(BigDecimal(10), "test1")
        } yield result).runSync should ===(Allowed)
      println("G " +Instant.now())
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
