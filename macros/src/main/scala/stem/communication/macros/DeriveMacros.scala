package stem.communication.macros

import scodec.bits.BitVector
import stem.communication.macros.BoopickleCodec.codec
import zio.Task

import scala.reflect.ClassTag
import scala.reflect.macros.blackbox

class DeriveMacros(c: blackbox.Context) {

  import c.internal._
  import c.universe._

  /** A reified method definition with some useful methods for transforming it. */
  case class Method(m: MethodSymbol,
                    tps: List[TypeDef],
                    pss: List[List[ValDef]],
                    rt: Type,
                    body: Tree) {
    def typeArgs: List[Type] = for (tp <- tps) yield typeRef(NoPrefix, tp.symbol, Nil)

    def paramLists(f: Type => Type): List[List[ValDef]] =
      for (ps <- pss)
        yield for (p <- ps) yield ValDef(p.mods, p.name, TypeTree(f(p.tpt.tpe)), p.rhs)

    def argLists(f: (TermName, Type) => Tree): List[List[Tree]] =
      for (ps <- pss)
        yield for (p <- ps) yield f(p.name, p.tpt.tpe)

    def definition: Tree = q"override def ${m.name}[..$tps](...$pss): $rt = $body"

  }

  /** Return the set of overridable members of `tpe`, excluding some undesired cases. */
  private def overridableMembersOf(tpe: Type): Iterable[Symbol] = {
    import definitions._
    val exclude = Set[Symbol](AnyClass, AnyRefClass, AnyValClass, ObjectClass)
    tpe.members.filterNot(
      m =>
        m.isConstructor || m.isFinal || m.isImplementationArtifact || m.isSynthetic || exclude(
          m.owner
        )
    )
  }

  private def overridableMethodsOf(algebra: Type): Iterable[Method] =
    for (member <- overridableMembersOf(algebra) if member.isMethod && !member.asMethod.isAccessor)
      yield {
        val method = member.asMethod
        val signature = method.typeSignatureIn(algebra)
        val typeParams = for (tp <- signature.typeParams) yield typeDef(tp)
        val paramLists = for (ps <- signature.paramLists)
          yield
            for (p <- ps) yield {
              // Only preserve the implicit modifier (e.g. drop the default parameter flag).
              val modifiers = if (p.isImplicit) Modifiers(Flag.IMPLICIT) else Modifiers()
              ValDef(modifiers, p.name.toTermName, TypeTree(p.typeSignatureIn(algebra)), EmptyTree)
            }

        Method(
          method,
          typeParams,
          paramLists,
          signature.finalResultType,
          q"_root_.scala.Predef.???"
        )
      }

  def stubMethods(methods: Iterable[Method]): Iterable[c.universe.Tree] = ???

  def derive[Algebra, State, Event, Reject](
                                             implicit tag: c.WeakTypeTag[Algebra],
                                             statetag: c.WeakTypeTag[State],
                                             eventtag: c.WeakTypeTag[Event],
                                             rejecttag: c.WeakTypeTag[Reject]
                                           ): c.Tree = {
    import c.universe._

    val algebra: c.universe.Type = tag.tpe.typeConstructor.dealias
    val state: c.universe.Type = statetag.tpe.typeConstructor.dealias
    val event: c.universe.Type = tag.tpe.typeConstructor.dealias
    val reject: c.universe.Type = tag.tpe.typeConstructor.dealias
    val methods: Iterable[Method] = overridableMethodsOf(algebra)
val stubbedMethods: Iterable[Tree] = stubMethods(methods)


    // function hint, bitvector to Task[bitvector]
    val serverHintBitVectorFunction: Tree = {
      val hint = ???
      val arguments: BitVector = ???




      val codecInput = codec[(BigDecimal, String)]
      val codecResult = codec[LockResponse]
      input
      <- Task.fromTry(codecInput.decodeValue(arguments).toTry).mapError(errorHandler)
      result
      <- (algebra.lock _).tupled(input)
      vector
      <- Task.fromTry(codecResult.encode(result).toTry)

      ???
    }

    q""" new StemProtocol[$algebra, $state, $event, $reject] {
             private val mainCodec = codec[(Int, BitVector)]
             val client: (BitVector => Task[BitVector], Throwable => $reject) => $algebra =
               (commFn: BitVector => Task[BitVector], errorHandler: Throwable => String) =>
                 new $algebra { ..$methodBody }

             val server: ($algebra, Throwable => $reject) => Invocation[$state, $event, $reject] =
               (algebra: $algebra, errorHandler: Throwable => $reject) =>
                 new Invocation[$state, $event, $reject] {
                    private def buildVectorFromHint(hint: Int, arguments: BitVector): Task[BitVector] = { $serverHintBitVectorFunction }

                   override def call(message: BitVector): ZIO[Has[AlgebraCombinators[$state, $event, $reject]], $reject, BitVector] = {
                     // for each method extract the name, it could be a sequence number for the method
                     ZIO.accessM { algebraOps =>
                       // according to the hint, extract the arguments
                       for {
                         element <- Task.fromTry(mainCodec.decodeValue(message).toTry).mapError(errorHandler)
                         (hint, arguments) = element
                         //use extractedHint to decide what to do here
                         vector <- buildVectorFromHint(hint, arguments).mapError(errorHandler)
                       } yield vector
                     }
                   }
               }
           }"""
  }

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
