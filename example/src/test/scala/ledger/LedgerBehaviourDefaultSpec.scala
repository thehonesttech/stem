package ledger

import ledger.Converters.toLedgerBigDecimal
import ledger.LedgerEntity.LedgerCommandHandler
import ledger.eventsourcing.events.events.{AmountLocked, LedgerEvent}
import stem.data.EventTag
import stem.data.Tagging.Const
import stem.runtime.akka.EventSourcedBehaviour
import stem.test.StemOps
import stem.test.TestStemRuntime._
import zio.Cause
import zio.test.Assertion.{equalTo, hasSameElements}
import zio.test._

object LedgerBehaviourDefaultSpec extends DefaultRunnableSpec with StemOps {

  def spec =
    suite("Ledger stemtity")(
      testM("receives commands, produces events and update state") {
        val key = "key"
        for {
          StemtityAndProbe(ledgers, probe) <- buildLedgerStemtity
          result                           <- ledgers(key).lock(BigDecimal(10), "test1")
          events                           <- probe(key).events
          stateInitial                     <- probe(key).state
          _                                <- ledgers(key).lock(BigDecimal(12), "test2")
          events2                          <- probe(key).events
          stateSecondCall                  <- probe(key).state
        } yield {
          assert(result)(equalTo(Allowed)) &&
          assert(events)(hasSameElements(List(AmountLocked(toLedgerBigDecimal(BigDecimal(10)), "test1")))) &&
          assert(stateInitial)(equalTo(1)) &&
          assert(events2)(
            hasSameElements(List(AmountLocked(toLedgerBigDecimal(BigDecimal(10)), "test1"), AmountLocked(toLedgerBigDecimal(BigDecimal(12)), "test2")))
          ) &&
          assert(stateSecondCall)(equalTo(2))
        }
      },
      testM("End to end test") {
        failed(Cause.empty)
      }
    ).provideLayerShared(testLayer[Int, LedgerEvent, String])

  private def buildLedgerStemtity =
    memoryStemtity[String, LedgerCommandHandler, Int, LedgerEvent, String](
      Const(EventTag("testKey")),
      EventSourcedBehaviour(new LedgerCommandHandler(), LedgerEntity.eventHandlerLogic, LedgerEntity.errorHandler)
    )
}
