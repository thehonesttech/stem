package stem.communication.macros

import boopickle.Default._
import stem.communication.macros.BoopickleCodec.{codec, _}
import ledger.LedgerEntity.LedgerCommandHandler
import ledger.LockResponse
import ledger.eventsourcing.events.events.LedgerEvent
import scodec.bits.BitVector
import stem.data.{AlgebraCombinators, Invocation, StemProtocol}
import zio.{Has, IO, Task, ZIO}

@deprecated("Use RpcMacro")
object LedgerRpcMacro {

  implicit val ledgerProtocol: StemProtocol[LedgerCommandHandler, Int, LedgerEvent, String] =
    new StemProtocol[LedgerCommandHandler, Int, LedgerEvent, String] {
      private val mainCodec = codec[(Int, BitVector)]
      val client: (BitVector => IO[String, BitVector], Throwable => String) => LedgerCommandHandler =
        (commFn: BitVector => IO[String, BitVector], errorHandler: Throwable => String) =>
          new LedgerCommandHandler {
            override def lock(amount: BigDecimal, idempotencyKey: String): SIO[LockResponse] = {
              ZIO.accessM { _: Has[AlgebraCombinators[Int, LedgerEvent, String]] =>
                val hint = 1

                val tuple: (BigDecimal, String) = (amount, idempotencyKey)

                // if method has a protobuf message, use it, same for response otherwise use boopickle protocol
                // LockReply.validate()

                val codecInput = codec[(BigDecimal, String)]
                val codecResult = codec[LockResponse]

                (for {
                  tupleEncoded <- IO.fromTry(codecInput.encode(tuple).toTry).mapError(errorHandler)
                  // start common code
                  arguments <- IO.fromTry(mainCodec.encode(hint -> tupleEncoded).toTry).mapError(errorHandler)
                  vector <- commFn(arguments)
                  // end of common code
                  decoded <- IO.fromTry(codecResult.decodeValue(vector).toTry).mapError(errorHandler)
                } yield decoded)
              }

            }

            override def release(transactionId: String, idempotencyKey: String): SIO[Unit] = {
              ???
            }

            override def clear(transactionId: String, idempotencyKey: String): SIO[Unit] = {
              ???
            }


          }


      val server: (LedgerCommandHandler, Throwable => String) => Invocation[Int, LedgerEvent, String] =
        (algebra: LedgerCommandHandler, errorHandler: Throwable => String) =>
          new Invocation[Int, LedgerEvent, String] {
            val codecInput = codec[(BigDecimal, String)]
            val codecResult = codec[LockResponse]

            private def macroInvocation(arguments: BitVector): ZIO[Has[AlgebraCombinators[Int, LedgerEvent, String]], String, BitVector] = {
              for {
                input <- Task.fromTry(codecInput.decodeValue(arguments).toTry).mapError(errorHandler)
                result <- (algebra.lock _).tupled(input)
                vector <- Task.fromTry(codecResult.encode(result).toTry).mapError(errorHandler)
              } yield vector
            }

            override def call(message: BitVector): ZIO[Has[AlgebraCombinators[Int, LedgerEvent, String]], String, BitVector] = {
              // for each method extract the name, it could be a sequence number for the method
              // according to the hint, extract the arguments
              for {
                element <- Task.fromTry(mainCodec.decodeValue(message).toTry).mapError(errorHandler)
                (hint, arguments) = element
                //use extractedHint to decide what to do here
                vector <- macroInvocation(arguments)
              } yield vector
            }
          }
    }

}
