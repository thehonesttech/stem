package stem.communication.macros

import org.scalatest.freespec.AnyFreeSpec
import zio.Task

import scala.reflect.ClassTag

class DeriveMacrosSpec extends AnyFreeSpec {

  "Client macro" - {
    "called directly" in {
      RpcMacro.client[AlgebraImpl, String](vector => Task.succeed(vector), AlgebraImpl.errorHandler)
    }

    "called with generic type" in {
//      callMacro[AlgebraImpl]
    }
  }

  private def callMacro[Algebra] = {
    RpcMacro.client[Algebra, String](vector => Task.succeed(vector), AlgebraImpl.errorHandler)
  }

}

class AlgebraImpl {
  def operation1(param: String): Task[String] = Task.succeed(param)
}

object AlgebraImpl {
  val errorHandler: Throwable => String = _.getMessage
}
