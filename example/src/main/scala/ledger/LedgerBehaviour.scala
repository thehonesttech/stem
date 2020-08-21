package ledger

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import io.grpc.Status
import ledger.LedgerEntity.LedgerCommandHandler
import ledger.communication.grpc.service.ZioService.ZLedger
import ledger.communication.grpc.service._
import ledger.eventsourcing.events.events
import ledger.eventsourcing.events.events.{AmountLocked, LedgerEvent, LockReleased}
import scalapb.zio_grpc.{ServerMain, ServiceList}
import stem.StemApp
import stem.annotations.MethodId
import stem.communication.macros.RpcMacro
import stem.data.AlgebraCombinators.Combinators
import stem.data.{AlgebraCombinators, StemProtocol}
import stem.runtime.akka.StemRuntime.memoryStemtity
import stem.runtime.akka._
import stem.runtime.{AlgebraTransformer, Fold}
import stem.tagging.{EventTag, Tagging}
import zio.{Has, Managed, Runtime, Task, ZEnv, ZIO, ZLayer}

sealed trait LockResponse
case object Allowed extends LockResponse
case class Denied(reason: String) extends LockResponse

object LedgerServer extends ServerMain {

  private val actorSystem =
    ZLayer.fromManaged(
      Managed.make(Task(ActorSystem("System", ConfigFactory.load("stem.conf"))))(sys => Task.fromFuture(_ => sys.terminate()).either)
    )

  private val runtimeSettings = actorSystem to ZLayer.fromService { actorSystem: ActorSystem =>
      RuntimeSettings.default(actorSystem)
    }

  private val liveAlgebra = StemApp.liveAlgebraLayer[Int, LedgerEvent, String]

  // dependency injection wiring
  private def buildSystem[R]: ZLayer[R, Throwable, Has[ZLedger[ZEnv, Any]]] =
    ((actorSystem and runtimeSettings to LedgerEntity.live) and liveAlgebra to LedgerService.live)

  override def services: ServiceList[zio.ZEnv] = ServiceList.addManaged(buildSystem.build.map(_.get))
}

// you can have multiples entities
object LedgerEntity {
  implicit val runtime: Runtime[zio.ZEnv] = LedgerServer
  implicit val keyEncoder: KeyEncoder[String] = (a: String) => a
  implicit val keyDecoder: KeyDecoder[String] = (key: String) => Some(key)

  class LedgerCommandHandler {
    type SIO[Response] = ZIO[Combinators[Int, LedgerEvent, String], String, Response]

    @MethodId(1)
    def lock(amount: BigDecimal, idempotencyKey: String): SIO[LockResponse] = ZIO.accessM { opsL =>
      val ops = opsL.get
      import ops._
      (for {
        state <- read
        _     <- append(AmountLocked(amount = Some(toLedgerBigDecimal(amount)), idempotencyKey = idempotencyKey))
      } yield Allowed).mapError(errorHandler)
    }

    @MethodId(2)
    def release(transactionId: String, idempotencyKey: String): SIO[Unit] = ???

    @MethodId(3)
    def clear(transactionId: String, idempotencyKey: String): SIO[Unit] = ???

    private def toLedgerBigDecimal(bigDecimal: BigDecimal): events.BigDecimal =
      ledger.eventsourcing.events.events.BigDecimal(bigDecimal.scale, bigDecimal.precision)
  }

  private val errorHandler: Throwable => String = error => error.getMessage

  val eventHandlerLogic: Fold[Int, LedgerEvent] = Fold(
    initial = 0,
    reduce = {
      case (oldState, event) =>
        val newState = event match {
          case _: AmountLocked =>
            if (oldState % 2 == 0) oldState + 1
            else oldState + 5
          case _: LockReleased => 2
          case _               => 3
        }
        Task.succeed(newState)
    }
  )

  implicit val ledgerProtocol
    : StemProtocol[LedgerCommandHandler, Int, LedgerEvent, String] = RpcMacro.derive[LedgerCommandHandler, Int, LedgerEvent, String] //  LedgerRpcMacro.ledgerProtocol
  // TODO: setup kafka
  val live = ZLayer.fromEffect {
    memoryStemtity[String, LedgerCommandHandler, Int, LedgerEvent, String](
      "Ledger",
      Tagging.const(EventTag("Ledger")),
      EventSourcedBehaviour(new LedgerCommandHandler(), eventHandlerLogic, errorHandler)
    )
  }
}

object LedgerService {

  type Ledgers = String => LedgerCommandHandler

  object Conversions {

    implicit def toLedgerBigDecimal(bigDecimal: BigDecimal): events.BigDecimal =
      ledger.eventsourcing.events.events.BigDecimal(bigDecimal.scale, bigDecimal.precision)

    implicit def fromLedgerBigDecimal(bigDecimal: Option[events.BigDecimal]): BigDecimal = {
      bigDecimal.map(el => BigDecimal.apply(el.scale, el.precision)).getOrElse(BigDecimal(0))
    }
  }

  val live =
    ZLayer.fromServices { (ledgers: Ledgers, algebra: AlgebraCombinators[Int, LedgerEvent, String]) =>
      val ledgerService = new ZioService.ZLedger[ZEnv with Combinators[Int, LedgerEvent, String], Any] {
        import Conversions._
        import zio.console._

        override def lock(request: LockRequest): ZIO[ZEnv with Combinators[Int, LedgerEvent, String], Status, LockReply] = {
          (for {
            reply <- ledgers(request.id)
              .lock(request.amount, request.idempotencyKey)
            _ <- putStrLn(reply.toString)
          } yield LockReply().withMessage(reply.toString))
            .mapError(_ => Status.NOT_FOUND)
        }

        override def release(request: ReleaseRequest): ZIO[ZEnv with Combinators[Int, LedgerEvent, String], Status, ReleaseReply] =
          ???

        override def clear(request: ClearRequest): ZIO[ZEnv with Combinators[Int, LedgerEvent, String], Status, ClearReply] = ???
      }
      AlgebraTransformer.withAlgebra(ledgerService, algebra)
    }

}
