package transactions

import accounts.{AccountId, AccountTransactionId}
import ledger.LedgerServer
import ledger.eventsourcing.events.TransactionStatus.Succeeded
import ledger.eventsourcing.events._
import scalapb.TypeMapper
import stem.StemApp
import stem.communication.macros.RpcMacro
import stem.communication.macros.annotations.MethodId
import stem.data.AlgebraCombinators._
import stem.data.{EventTag, StemProtocol, Tagging}
import stem.runtime.Fold
import stem.runtime.Fold.impossible
import stem.runtime.akka.StemRuntime.memoryStemtity
import stem.runtime.akka.{EventSourcedBehaviour, KeyDecoder, KeyEncoder}
import zio.{IO, Runtime, Task}

object TransactionEntity {
  implicit val runtime: Runtime[zio.ZEnv] = LedgerServer

  class TransactionCommandHandler {

    type SIO[Response] = StemApp.SIO[TransactionState, TransactionEvent, String, Response]

    @MethodId(1)
    def create(from: AccountId, to: AccountId, amount: BigDecimal): SIO[Unit] = accessCombinator { ops =>
      import ops._
      read
        .flatMap {
          case _: ActiveTransaction =>
            append(TransactionCreated(from = from, to = to, amount = Some(amount)))
          case _ =>
            ignore
        }
    }

    @MethodId(2)
    def authorise: SIO[Unit] = accessCombinator { ops =>
      import ops._
      read.flatMap {
        case _: InitialTransaction =>
          append(TransactionAuthorized())
        case _ =>
          reject("Auth in progress, cannot auth twice")
      }

    }

    @MethodId(3)
    def fail(reason: String): SIO[Unit] = accessCombinator { ops =>
      import ops._

      read.flatMap {
        case inProgress: ActiveTransaction =>
          if (inProgress.status == TransactionStatus.Failed) {
            ignore
          } else {
            append(TransactionFailed(reason))
          }
        case _ =>
          reject("Transaction not found")
      }
    }

    @MethodId(4)
    def succeed: SIO[Unit] = accessCombinator { ops =>
      import ops._
      read.flatMap {
        case inProgress: ActiveTransaction =>
          if (inProgress.status == TransactionStatus.Succeeded) {
            ignore
          } else if (inProgress.status == TransactionStatus.Authorized) {
            append(TransactionSucceeded())
          } else {
            reject("Illegal transition")
          }
        case _ =>
          reject("Transaction not found")
      }
    }

    @MethodId(4)
    def getInfo: SIO[TransactionInfo] = accessCombinator { ops =>
      import ops._
      read.flatMap {
        case ActiveTransaction(amount, from, to, status) =>
          IO.succeed(TransactionInfo(from, to, amount.getOrElse(BigDecimal(0)), status == Succeeded))
        case _ =>
          reject("Transaction not found")
      }
    }
  }

  val errorHandler: Throwable => String = _.getMessage

  val eventHandlerLogic: Fold[TransactionState, TransactionEvent] = Fold(
    initial = InitialTransaction(),
    reduce = {
      case (InitialTransaction(), TransactionCreated(from, to, amount)) => IO.succeed(ActiveTransaction(amount, from, to, TransactionStatus.Requested))
      case (state: ActiveTransaction, TransactionAuthorized())          => IO.succeed(state.copy(status = TransactionStatus.Authorized))
      case _                                                            => impossible
    }
  )

  implicit val transactionProtocol: StemProtocol[TransactionCommandHandler, TransactionState, TransactionEvent, String] =
    RpcMacro.derive[TransactionCommandHandler, TransactionState, TransactionEvent, String]

  val tagging = Tagging.const(EventTag("Transaction"))

  val live = memoryStemtity[TransactionId, TransactionCommandHandler, TransactionState, TransactionEvent, String](
    "Transaction",
    tagging,
    EventSourcedBehaviour(new TransactionCommandHandler(), eventHandlerLogic, errorHandler)
  ).toLayer
}

case class TransactionId(value: String) extends AnyVal

object TransactionId {
  implicit val typeMapper = TypeMapper(TransactionId.apply)(_.value)
  implicit val keyEncoder: KeyEncoder[TransactionId] = (a: TransactionId) => a.value
  implicit val keyDecoder: KeyDecoder[TransactionId] = (key: String) => Some(TransactionId(key))
}

final case class TransactionInfo(fromAccountId: AccountId, toAccountId: AccountId, amount: BigDecimal, succeeded: Boolean)
