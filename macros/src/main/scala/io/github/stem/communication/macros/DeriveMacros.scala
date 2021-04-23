package io.github.stem.communication.macros

import scodec.bits.BitVector

import scala.reflect.macros.blackbox
import io.github.stem.communication.macros.annotations.MethodId

class DeriveMacros(val c: blackbox.Context) {

  import c.internal._
  import c.universe._

  /** A reified method definition with some useful methods for transforming it. */
  case class Method(
    m: MethodSymbol,
    typeParams: List[TypeDef],
    paramList: List[List[ValDef]],
    returnType: Type,
    body: Tree,
    hint: Option[Int] = None
  ) {
    def typeArgs: List[Type] = for (tp <- typeParams) yield typeRef(NoPrefix, tp.symbol, Nil)

    def paramLists(f: Type => Type): List[List[ValDef]] =
      for (ps <- paramList)
        yield for (p <- ps) yield ValDef(p.mods, p.name, TypeTree(f(p.tpt.tpe)), p.rhs)

    def argLists(f: (TermName, Type) => Tree): List[List[Tree]] =
      for (ps <- paramList)
        yield for (p <- ps) yield f(p.name, p.tpt.tpe)

    def definition: Tree = q"override def ${m.name}[..$typeParams](...$paramList): $returnType = $body"

  }

  /** Return the set of overridable members of `tpe`, excluding some undesired cases. */
  private def overridableMembersOf(tpe: Type): Iterable[Symbol] = {
    import definitions._
    val exclude = Set[Symbol](AnyClass, AnyRefClass, AnyValClass, ObjectClass)
    tpe.members.filterNot(m =>
      m.isConstructor || m.isFinal || m.isImplementationArtifact || m.isSynthetic || exclude(
        m.owner
      )
    )
  }

  private def overridableMethodsOf(algebra: Type): Iterable[Method] =
    for (member <- overridableMembersOf(algebra) if member.isMethod && member.asMethod.isPublic && !member.asMethod.isAccessor)
      yield {
        val method = member.asMethod
        val methodIdValue = member.annotations.collectFirst {
          case a if a.tree.tpe.dealias <:< typeOf[MethodId].dealias =>
            val Literal(Constant(value: Int)) = a.tree.children.tail.head
            value
        }
        val signature = method.typeSignatureIn(algebra)
        val typeParams = for (tp <- signature.typeParams) yield typeDef(tp)
        val paramLists =
          for (ps <- signature.paramLists)
            yield for (p <- ps) yield {
              // Only preserve the implicit modifier (e.g. drop the default parameter flag).
              val modifiers = if (p.isImplicit) Modifiers(Flag.IMPLICIT) else Modifiers()
              ValDef(modifiers, p.name.toTermName, TypeTree(p.typeSignatureIn(algebra)), EmptyTree)
            }

        Method(
          method,
          typeParams,
          paramLists,
          signature.finalResultType,
          q"_root_.scala.Predef.???",
          hint = methodIdValue
        )
      }

  def stubMethodsForClient(
    methods: Iterable[Method],
    state: c.universe.Type,
    event: c.universe.Type,
    reject: c.universe.Type
  ): Iterable[c.universe.Tree] = {
    methods.zipWithIndex.map { case (method @ Method(_, _, paramList, TypeRef(_, _, outParams), _, hint), index) =>
//        println(s"OutParams $outParams on method $method")

      val out = outParams.last
      val paramTypes = paramList.flatten.map(_.tpt)
      val args = method.argLists((pn, _) => Ident(pn)).flatten
      val hintToUse: String = hint.getOrElse(index).toString

      val code = if (args.isEmpty) {
        q"""
             (for {
                    // start common code
                    arguments <- Task.fromTry(mainCodec.encode(hint -> BitVector.empty).toTry).mapError(errorHandler)
                    vector    <- commFn(arguments).mapError(errorHandler)
                    // end of common code
                    decoded <- IO.fromTry(codecResult.decodeValue(vector).toTry).mapError(errorHandler)
                    result <- ZIO.fromEither(decoded)
               } yield result) 
          """
      } else {
        val TupleNCons = TypeName(s"Tuple${paramTypes.size}")
        val TupleNConsTerm = TermName(s"Tuple${paramTypes.size}")
        q"""val codecInput = codec[$TupleNCons[..$paramTypes]]
              val tuple: $TupleNCons[..$paramTypes] = $TupleNConsTerm(..$args)
             
              // if method has a protobuf message, use it, same for response otherwise use boopickle protocol
              (for {
                    tupleEncoded <- IO.fromTry(codecInput.encode(tuple).toTry).mapError(errorHandler)
             
                    // start common code
                    arguments <- Task.fromTry(mainCodec.encode(hint -> tupleEncoded).toTry).mapError(errorHandler)
                    vector    <- commFn(arguments).mapError(errorHandler)
                    // end of common code
                    decoded <- IO.fromTry(codecResult.decodeValue(vector).toTry).mapError(errorHandler)
                    result <- ZIO.fromEither(decoded)
               } yield result)"""

      }

      val newBody =
        q""" ZIO.accessM { _: Has[Combinators[$state, $event, $reject]] =>
                       val hint = $hintToUse
                       
                       val codecResult = codec[Either[$reject, $out]]
                       
                       ..$code
                     }"""
      method.copy(body = newBody).definition
    }
  }

  def derive[Algebra, State, Event, Reject](implicit
    algebraTag: c.WeakTypeTag[Algebra],
    statetag: c.WeakTypeTag[State],
    eventtag: c.WeakTypeTag[Event],
    rejecttag: c.WeakTypeTag[Reject]
  ): c.Tree = {
    import c.universe._

    val algebra: c.universe.Type = algebraTag.tpe.typeConstructor.dealias
    val state: c.universe.Type = statetag.tpe.typeConstructor.dealias
    val event: c.universe.Type = eventtag.tpe.typeConstructor.dealias
    val reject: c.universe.Type = rejecttag.tpe.typeConstructor.dealias
    val methods: Iterable[Method] = overridableMethodsOf(algebra)
    val stubbedMethods: Iterable[Tree] = stubMethodsForClient(methods, state, event, reject)
    // function hint, bitvector to Task[bitvector]
    val serverHintBitVectorFunction: Tree = {
      methods.zipWithIndex.foldLeft[Tree](q"""throw new IllegalArgumentException(s"Unknown type tag $$hint")""") { case (acc, (method, index)) =>
        val Method(name, _, paramList, TypeRef(_, _, outParams), _, hint) = method

        val hintToUse = hint.getOrElse(index).toString

        val out = outParams.last
        val argList = paramList.map(x => (1 to x.size).map(i => q"args.${TermName(s"_$i")}"))
        val argsTerm =
          if (argList.isEmpty) q""
          else {
            val paramTypes = paramList.flatten.map(_.tpt)
            val TupleNCons = TypeName(s"Tuple${paramTypes.size}")

            q"""
               val codecInput = codec[$TupleNCons[..$paramTypes]]
               """
          }

        def runImplementation =
          if (argList.isEmpty)
            q"algebra.$name.either"
          else
            q"algebra.$name(...$argList).either"

        val codecInputCode =
          if (argList.isEmpty) q"Task.unit"
          else
            q"""Task.fromTry(codecInput.decodeValue(arguments).toTry)"""
        val invocation =
          q"""
              val codecResult = codec[Either[$reject, $out]]
              ..$argsTerm
              for {
                  args <- $codecInputCode
                  result <- $runImplementation
                  vector <- Task.fromTry(codecResult.encode(result).toTry)
              } yield vector
              """
        q"""
             if (hint == $hintToUse) { $invocation } else $acc"""
      }
    }

    q""" new StemProtocol[$algebra, $state, $event, $reject] {
            import scodec.bits.BitVector
            import boopickle.Default._
            import io.github.stem.communication.macros.BoopickleCodec._
            import io.github.stem.data.Invocation
            import zio._
            import io.github.stem.data.AlgebraCombinators
            import io.github.stem.data.Combinators

             private val mainCodec = codec[(String, BitVector)]
             val client: (BitVector => Task[BitVector], Throwable => $reject) => $algebra =
               (commFn: BitVector => Task[BitVector], errorHandler: Throwable => $reject) =>
                 new $algebra { ..$stubbedMethods }

             val server: ($algebra, Throwable => $reject) => Invocation[$state, $event, $reject] =
               (algebra: $algebra, errorHandler: Throwable => $reject) =>
                 new Invocation[$state, $event, $reject] {
                   private def buildVectorFromHint(hint: String, arguments: BitVector): ZIO[Has[Combinators[$state, $event, $reject]], Throwable, BitVector] = { $serverHintBitVectorFunction }

                   override def call(message: BitVector): ZIO[Has[Combinators[$state, $event, $reject]], Throwable, BitVector] = {
                     // for each method extract the name, it could be a sequence number for the method
                       // according to the hint, extract the arguments
                       for {
                         element <- Task.fromTry(mainCodec.decodeValue(message).toTry)
                         hint = element._1
                         arguments = element._2
                         //use extractedHint to decide what to do here
                         vector <- buildVectorFromHint(hint, arguments)
                       } yield vector
                     }
               }
           }"""
  }
}
