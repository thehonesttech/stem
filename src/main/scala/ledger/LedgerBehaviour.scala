package ledger

import akka.actor.ActorSystem
import io.grpc.Status
import ledger.LedgerService.Service.Ledgers
import ledger.LedgersEntity.LedgerCommandHandler
import ledger.communication.grpc.service._
import ledger.eventsourcing.events.events
import ledger.eventsourcing.events.events.{AmountLocked, LedgerEvent, LockReleased}
import scalapb.zio_grpc.{ServerMain, ServiceList}
import stem.StemApp
import stem.engine.AlgebraCombinators.Combinators
import stem.engine.akka.StemRuntime.memoryStemtity
import stem.engine.akka._
import stem.engine.{AlgebraCombinators, Fold}
import stem.tagging.{EventTag, Tagging}
import zio.{Has, Managed, Runtime, Task, ULayer, ZEnv, ZIO, ZLayer}

sealed trait LockResponse
case object Allowed extends LockResponse
case class Denied(reason: String) extends LockResponse

object LedgerServer extends ServerMain {

  private val actorSystem =
    ZLayer.fromManaged(Managed.make(Task(ActorSystem("System")))(sys => Task.fromFuture(_ => sys.terminate()).either))

  private val runtimeSettings = actorSystem to ZLayer.fromFunction { actorSystem: Has[ActorSystem] =>
      RuntimeSettings.default(actorSystem.get)
    }

  private val liveAlgebra: ULayer[Combinators[Int, LedgerEvent, String]] = ZLayer.succeed(StemApp.emptyAlgebra[Int, LedgerEvent, String])

  // dependency injection wiring
  private val buildSystem = {
    ((ZEnv.live and actorSystem and runtimeSettings to LedgersEntity.live) and liveAlgebra to LedgerService.live).build.use { layer =>
      ZIO.access[ZEnv] { _ =>
        layer.get
      }
    }
  }

  override def services: ServiceList[zio.ZEnv] =
    ServiceList.addM(buildSystem)
}

object LedgersEntity {
  implicit val runtime: Runtime[zio.ZEnv] = LedgerServer
  implicit val keyEncoder: KeyEncoder[String] = (a: String) => a
  implicit val keyDecoder: KeyDecoder[String] = (key: String) => Some(key)

  class LedgerCommandHandler {
    type SIO[Response] = ZIO[Combinators[Int, LedgerEvent, String], String, Response]

    def lock(amount: BigDecimal, idempotencyKey: String): SIO[LockResponse] = ZIO.accessM { opsL =>
      val ops = opsL.get
      import ops._
      (for {
        state <- read
        _     <- append(AmountLocked(amount = Some(toLedgerBigDecimal(amount)), idempotencyKey = idempotencyKey))
      } yield Allowed).mapError(errorHandler)
    }

    def release(transactionId: String, idempotencyKey: String): SIO[Unit] = ???

    def clear(transactionId: String, idempotencyKey: String): SIO[Unit] = ???

    private def toLedgerBigDecimal(bigDecimal: BigDecimal): events.BigDecimal =
      ledger.eventsourcing.events.events.BigDecimal(bigDecimal.scale, bigDecimal.precision)
  }

  private val errorHandler: Throwable => String = error => error.getMessage

  object LedgerEventHandler {
    private val initialState = 0

    val eventHandlerLogic: Fold[Int, LedgerEvent] = Fold(initialState, reduce = {
      case (state, event) =>
        val newState = event match {
          case _: AmountLocked => 1
          case _: LockReleased => 2
          case _               => 3
        }
        Task.succeed(newState)
    })
  }

  private val eventSourcedBehaviour: EventSourcedBehaviour[LedgerCommandHandler, Int, LedgerEvent, String] =
    EventSourcedBehaviour(new LedgerCommandHandler(), LedgerEventHandler.eventHandlerLogic, errorHandler)

  val live = ZLayer.fromEffect {
    memoryStemtity[String, LedgerCommandHandler, Int, LedgerEvent, String](
      "Ledger",
      Tagging.const(EventTag("Ledger")),
      eventSourcedBehaviour
    )
  }
}

object LedgerService {

  type Service = ZioService.ZLedger[ZEnv with Combinators[Int, LedgerEvent, String], Any]

  object Service {
    type Ledgers = String => LedgerCommandHandler

    implicit def toLedgerBigDecimal(bigDecimal: BigDecimal): events.BigDecimal =
      ledger.eventsourcing.events.events.BigDecimal(bigDecimal.scale, bigDecimal.precision)

    implicit def fromLedgerBigDecimal(bigDecimal: Option[events.BigDecimal]): BigDecimal = {
      bigDecimal.map(el => BigDecimal.apply(el.scale, el.precision)).getOrElse(BigDecimal(0))
    }
  }

  val live =
    ZLayer.fromServices { (ledgers: Ledgers, algebra: AlgebraCombinators[Int, LedgerEvent, String]) =>
      new ZioService.ZLedger[ZEnv, Any] {
        import Service._
        import zio.console._

        override def lock(request: LockRequest): ZIO[ZEnv, Status, LockReply] = {
          (for {
            reply <- ledgers(request.id)
              .lock(request.amount, request.idempotencyKey)
            _ <- putStrLn(reply.toString)
          } yield LockReply().withMessage(reply.toString))
            .mapError(_ => Status.NOT_FOUND)
            //TODO: I DON'T WANT TO DEAL WITH THIS (APART FROM TESTS)
            .provideCustomLayer(ZLayer.succeed(algebra))
        }

        override def release(request: ReleaseRequest): ZIO[ZEnv, Status, ReleaseReply] =
          ???

        override def clear(request: ClearRequest): ZIO[ZEnv, Status, ClearReply] = ???
      }
    }

}
