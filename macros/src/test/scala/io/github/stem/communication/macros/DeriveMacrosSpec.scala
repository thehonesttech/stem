package io.github.stem.communication.macros

import io.github.stem.data.{Combinators, StemProtocol}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers._
import zio.{Has, IO, ZIO}

class DeriveMacrosSpec extends AnyFreeSpec {

  "Client macro" - {
    "called proto macro" in {
      //      import scala.reflect.runtime.universe.showRaw
      val protocol: StemProtocol[AlgebraImpl, String, String, String] = RpcMacro.derive[AlgebraImpl, String, String, String]
    }
  }

  "Codec" - {
    import boopickle.Default._
    import io.github.stem.communication.macros.BoopickleCodec._
    import scodec.bits.BitVector

    val inputCodec = codec[(Int, String)]
    val mainCodec = codec[(String, BitVector)]

    "encode and decode" in {
      val input = (10, "hello")
      val inputEnc = inputCodec.encode(input).getOrElse(fail("Error"))
      val resultDec = inputCodec.decode(inputEnc).getOrElse(fail("Error")).value
      resultDec should ===(input)
      val encodedBitVector = ("1", inputEnc)
      val result = mainCodec.encode(encodedBitVector).getOrElse(fail("Error"))

      val decmain = mainCodec.decode(result).getOrElse(fail("Error")).value
      decmain should ===(encodedBitVector)
      val originalResult = inputCodec.decode(decmain._2).getOrElse(fail("Error")).value
      originalResult should ===(input)

    }
  }

}

class AlgebraImpl {
  type SIO[Return] = ZIO[Has[Combinators[String, String, String]], String, Return]

  def operation1(param: String): SIO[String] = IO.succeed(param)
}

object AlgebraImpl {
  val errorHandler: Throwable => String = _.getMessage
}
