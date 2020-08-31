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
import zio.test.environment.TestClock

class LedgerBehaviourSpec extends AnyFreeSpec with Matchers with TypeCheckedTripleEquals with StemOps {

  "In a Stem Ledger behaviour" - {
    val ledgerBehaviour =
      EventSourcedBehaviour(new LedgerCommandHandler(), LedgerEntity.eventHandlerLogic, LedgerEntity.errorHandler)
    "should be easy to test the entity not using actorsystem" in {
      // when a command happens, events are triggered and state updated
      println("Implementation start " + Instant.now())
      val (result, events) = (for {
        ledgerWithProbe <- memoryStemtity[String, LedgerCommandHandler, Int, LedgerEvent, String](Const(EventTag("testKey")), ledgerBehaviour)
          .mapError(_ => "Error")
        LedgerWithProbe(ledgers, probe) = ledgerWithProbe
        _ = println("Stemtity configured " + Instant.now())

        result <- ledgers("key").lock(BigDecimal(10), "test1")
        _      <- TestClock.adjust(1.seconds)
        events <- probe("key").getEvents.mapError(_ => "error")
//        _ <- ledgers("key2").lock(BigDecimal(10), "test1")
//        _ <- ledgers("key2").lock(BigDecimal(10), "testa1")
//        _ <- ledgers("key2").lock(BigDecimal(10), "tesat1")
//        _ <- ledgers("key3").lock(BigDecimal(10), "test1")
//        _ <- ledgers("key2").lock(BigDecimal(43), "otest")
      } yield result -> events).provideLayer(testLayer[Int, LedgerEvent, String]).runSync

      result should ===(Allowed)
      events should have size 1

      println("Implementation end " + Instant.now())
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
