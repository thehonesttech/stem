package ledger

import ledger.Converters.toLedgerBigDecimal
import ledger.LedgerEntity.LedgerCommandHandler
import ledger.eventsourcing.events.events.{AmountLocked, LedgerEvent}
import stem.StemApp
import stem.data.EventTag
import stem.data.Tagging.Const
import stem.runtime.akka.EventSourcedBehaviour
import stem.test.StemOps
import stem.test.TestStemRuntime._
import zio.{Cause, Task}
import zio.test.Assertion.{equalTo, hasSameElements}
import zio.test._

object LedgerBehaviourDefaultSpec extends DefaultRunnableSpec with StemOps {

  def spec =
    suite("Ledger stemtity")(
      testM("receives commands, produces events and update state") {
        val key = "key"
        for {
          StemtityAndProbe(ledgers, probe) <- ledgerStemtity
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
      test("End to end test") {
        //call grpc service, check events and state, advance time, read side view, send with kafka
//        for {
//        ledgerGrpcService <- ledgerGrpc.build.useNow.map(_.get)
//        result <- ledgerGrpcService.lock()
//
//        } yield ()
//
//        ledgerGrpc.build.useNow.map {
//
//        }

        assert(2)(equalTo(2))
      }
    ).provideLayerShared(testLayer[Int, LedgerEvent, String])


  // TODO avoid using layers, instead use zio
//  private val ledgerGrpc = ledgerStemtity.map(_.algebra).toLayer to LedgerGrpcService.live

  private val ledgerStemtity =
    memoryStemtity[String, LedgerCommandHandler, Int, LedgerEvent, String](
      Const(EventTag("testKey")),
      EventSourcedBehaviour(new LedgerCommandHandler(), LedgerEntity.eventHandlerLogic, LedgerEntity.errorHandler)
    )
}
