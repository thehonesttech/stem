package stem.communication.macros

import org.scalatest.freespec.AnyFreeSpec
import stem.data.StemProtocol
import zio.Task

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
  def operation1(param: String): Task[String] = Task.succeed(param)
}

object AlgebraImpl {
  val errorHandler: Throwable => String = _.getMessage
}
