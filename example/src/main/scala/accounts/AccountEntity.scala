package accounts

import stem.Converters._
import ledger.eventsourcing.events._
import ledger.{Allowed, LedgerServer, LockResponse}
import scalapb.TypeMapper
import stem.StemApp
import stem.communication.macros.RpcMacro
import stem.communication.macros.annotations.MethodId
import stem.data.AlgebraCombinators.accessCombinator
import stem.data.{EventTag, StemProtocol, Tagging}
import stem.runtime.Fold
import stem.runtime.akka.StemRuntime.memoryStemtity
import stem.runtime.akka.{EventSourcedBehaviour, KeyDecoder, KeyEncoder}
import zio.{Runtime, Task}

object AccountEntity {
  implicit val runtime: Runtime[zio.ZEnv] = LedgerServer

  class AccountCommandHandler {
    type SIO[Response] = StemApp.SIO[AccountState, AccountEvent, String, Response]

    @MethodId(1)
    def open: SIO[Unit] = accessCombinator { ops =>
      import ops._
      read
        .flatMap {
          case EmptyAccount() =>
            append(AccountOpened())
          case _ =>
            ignore
        }
        .mapError(errorHandler)
    }

    @MethodId(2)
    def credit(transactionId: AccountTransactionId, amount: BigDecimal): SIO[Unit] = accessCombinator { ops =>
      import ops._
      read.mapError(errorHandler).flatMap {
        case account: ActiveAccount =>
          if (account.processedTransactions(transactionId.value)) {
            ignore
          } else {
            append(AccountCredited(transactionId.value, Some(amount)))
          }.mapError(errorHandler)
        case _ =>
          reject("Account does not exist")
      }
    }

    @MethodId(3)
    def debit(transactionId: AccountTransactionId, amount: BigDecimal): SIO[Unit] = accessCombinator { ops =>
      import ops._
      read.mapError(errorHandler).flatMap {
        case account: ActiveAccount =>
          if (account.processedTransactions(transactionId.value)) {
            ignore
          } else {
            if (account.available.getOrElse(BigDecimal(0)) > amount) {
              append(AccountDebited(transactionId.value, Some(amount))).mapError(errorHandler)
            } else {
              reject("Insufficient funds")
            }
          }
        case _ =>
          reject("Account does not exist")
      }

    }

//    @MethodId(4)
//    def lock(amount: BigDecimal, idempotencyKey: String): SIO[LockResponse] = accessCombinator { ops =>
//      import ops._
//      (for {
//        state <- read
//        _     <- append(AmountLocked(amount = Some(amount), idempotencyKey = idempotencyKey))
//      } yield Allowed).mapError(errorHandler)
//    }

  }

  val errorHandler: Throwable => String = _.getMessage

  val eventHandlerLogic: Fold[AccountState, AccountEvent] = Fold(
    initial = EmptyAccount(),
    reduce = {
      case (oldState, event) =>
        val newState = event match {
          case locked: AmountLocked =>
            ActiveAccount(locked = locked.amount)
          case released: LockReleased =>
            EmptyAccount()
          case _ => EmptyAccount()
        }
        Task.succeed(newState)
    }
  )

  implicit val accountProtocol: StemProtocol[AccountCommandHandler, AccountState, AccountEvent, String] =
    RpcMacro.derive[AccountCommandHandler, AccountState, AccountEvent, String]

  val tagging = Tagging.const(EventTag("Ledger"))

  val live = memoryStemtity[AccountId, AccountCommandHandler, AccountState, AccountEvent, String](
    "Account",
    tagging,
    EventSourcedBehaviour(new AccountCommandHandler(), eventHandlerLogic, errorHandler)
  ).toLayer
}
case class AccountId(value: String) extends AnyVal

object AccountId {
  implicit val typeMapper = TypeMapper(AccountId.apply)(_.value)
  implicit val keyEncoder: KeyEncoder[AccountId] = (a: AccountId) => a.value
  implicit val keyDecoder: KeyDecoder[AccountId] = (key: String) => Some(AccountId(key))
}

case class AccountTransactionId(value: String) extends AnyVal

object AccountTransactionId {
  implicit val typeMapper = TypeMapper(AccountTransactionId.apply)(_.value)
}
