package stem.communication.internal.macros

import boopickle.Default._
import ledger.LedgerEntity.LedgerCommandHandler
import ledger.LockResponse
import ledger.eventsourcing.events.events.LedgerEvent
import scodec.bits.BitVector
import stem.communication.internal.macros.BoopickleCodec._
import stem.runtime.{AlgebraCombinators, Invocation}
import zio.{Has, Task, ZIO}

object RpcMacro {

  // TODO implement as macro
  def client[Algebra, Reject](fn: BitVector => Task[BitVector], errorHandler: Throwable => Reject): Algebra = ???

  // TODO implement as macro
  def server[Algebra, State, Event, Reject](algebra: Algebra): Invocation[State, Event, Reject] = ???

}

// example macro approach
//TODO do not use name, use hashcode of invocation instead in order to detect commands with same name but different methods
object LedgerRpcMacro {
  private val mainCodec = codec[(Int, BitVector)]

  def client(fn: BitVector => Task[BitVector], errorHandler: Throwable => String): LedgerCommandHandler = new LedgerCommandHandler {
    override def lock(amount: BigDecimal, idempotencyKey: String): SIO[LockResponse] = {
      ZIO.accessM { _: Has[AlgebraCombinators[Int, LedgerEvent, String]] =>
        val hint = 1

        val tuple: (BigDecimal, String) = (amount, idempotencyKey)
        val codecInput = codec[(BigDecimal, String)]
        val codecResult = codec[LockResponse]

        (for {
          tupleEncoded <- Task.fromTry(codecInput.encode(tuple).toTry)
          // start common code
          arguments <- Task.fromTry(mainCodec.encode(hint -> tupleEncoded).toTry)
          vector    <- fn(arguments)
          // end of common code
          decoded <- Task.fromTry(codecResult.decodeValue(vector).toTry)
        } yield decoded).mapError(errorHandler)
      }

    }

    override def release(transactionId: String, idempotencyKey: String): SIO[Unit] = { ??? }

    override def clear(transactionId: String, idempotencyKey: String): SIO[Unit] = { ??? }
  }

  def server(algebra: LedgerCommandHandler, errorHandler: Throwable => String): Invocation[Int, LedgerEvent, String] = {
    new Invocation[Int, LedgerEvent, String] {
      override def call(message: BitVector): ZIO[Has[AlgebraCombinators[Int, LedgerEvent, String]], String, BitVector] = {
        // for each method extract the name, it could be a sequence number for the method
        ZIO.accessM { algebraOps =>
          // according to the hint, extract the arguments
          for {
            element <- Task.fromTry(mainCodec.decodeValue(message).toTry).mapError(errorHandler)
            (hint, arguments) = element
            //use extractedHint to decide what to do here
            codecInput = codec[(BigDecimal, String)]
            codecResult = codec[LockResponse]
            input  <- Task.fromTry(codecInput.decodeValue(arguments).toTry).mapError(errorHandler)
            result <- (algebra.lock _).tupled(input)
            vector <- Task.fromTry(codecResult.encode(result).toTry).mapError(errorHandler)
          } yield vector
        }
      }
    }
  }
}
