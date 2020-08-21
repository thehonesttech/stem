package stem.communication.macros

import org.scalatest.freespec.AnyFreeSpec
import stem.data.{AlgebraCombinators, StemProtocol}
import zio.{Has, IO, Task, ZIO}

import scala.reflect.ClassTag

class DeriveMacrosSpec extends AnyFreeSpec {

  "Client macro" - {
    "called proto macro" in {
//      import scala.reflect.runtime.universe.showRaw
      val protocol: StemProtocol[AlgebraImpl, String, String, String] = RpcMacro.derive[AlgebraImpl, String, String, String]
    }
  }

}

class AlgebraImpl {
  type SIO[Return] = ZIO[Has[AlgebraCombinators[String, String, String]], String, Return]

  def operation1(param: String): SIO[String] = IO.succeed(param)
}

object AlgebraImpl {
  val errorHandler: Throwable => String = _.getMessage
}
