package stem.communication.macros

import scodec.bits.BitVector
import zio.Task

import scala.reflect.ClassTag
import scala.reflect.macros.blackbox

object DeriveMacros {

  def client[Algebra, Reject](
    c: blackbox.Context
  )(fn: c.Expr[(BitVector) => Task[BitVector]], errorHandler: c.Expr[Throwable => Reject])(
    implicit tag: c.WeakTypeTag[Algebra]
  ): c.Tree = {
    import c.universe._
    def implement(algebra: Type, members: Iterable[Tree]): Tree = {
      q"new $algebra {..$members }"
    }

    val Alg: c.universe.Type = tag.tpe.typeConstructor.dealias
    implement(Alg, List.empty)
  }
//    instantiate(symbolOf[WireProtocol[Any]], Alg)(encoder(Alg), decoder(Alg))

}
