package stem.communication.macros

import scala.reflect.macros.blackbox
import stem.communication.macros.annotations.MethodId

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
    tpe.members.filterNot(
      m =>
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
    methods.zipWithIndex.map {
      case (method @ Method(_, _, paramList, TypeRef(_, _, outParams), _, hint), index) =>
//        println(s"OutParams $outParams on method $method")
        val out = outParams.last
        val paramTypes = paramList.flatten.map(_.tpt)
        val TupleNCons = TypeName(s"Tuple${paramTypes.size}")
        val TupleNConsTerm = TermName(s"Tuple${paramTypes.size}")
        val args = method.argLists((pn, _) => Ident(pn)).flatten

        val hintToUse: String = hint.getOrElse(index).toString

        // TODO: missing empty args
        val newBody =
          q""" ZIO.accessM { _: Has[AlgebraCombinators[$state, $event, $reject]] =>
                       val hint = $hintToUse

                       val codecInput = codec[$TupleNCons[..$paramTypes]]
                       val codecResult = codec[$out]
                       val tuple: $TupleNCons[..$paramTypes] = $TupleNConsTerm(..$args)

                       // if method has a protobuf message, use it, same for response otherwise use boopickle protocol
                       (for {
                         tupleEncoded <- IO.fromTry(codecInput.encode(tuple).toTry).mapError(errorHandler)

                         // start common code
                         arguments <- IO.fromTry(mainCodec.encode(hint -> tupleEncoded).toTry).mapError(errorHandler)
                         vector    <- commFn(arguments)
                         // end of common code
                         decoded <- IO.fromTry(codecResult.decodeValue(vector).toTry).mapError(errorHandler)
                       } yield decoded)
                     }"""
        method.copy(body = newBody).definition
    }
  }

  def derive[Algebra, State, Event, Reject](
    implicit algebraTag: c.WeakTypeTag[Algebra],
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
      methods.zipWithIndex.foldLeft[Tree](q"""throw new IllegalArgumentException(s"Unknown type tag $$hint")""") {
        case (acc, (method, index)) =>
          val Method(name, _, paramList, TypeRef(_, _, outParams), _, hint) = method

          val hintToUse = hint.getOrElse(index).toString

          val out = outParams.last
          val argList = paramList.map(x => (1 to x.size).map(i => q"args.${TermName(s"_$i")}"))
          val argsTerm =
            if (argList.isEmpty) q""
            else {
              val paramTypes = paramList.flatten.map(_.tpt)
              val TupleNCons = TypeName(s"Tuple${paramTypes.size}")
              // return tuple with try in the state of unpickle generic
              //we should unpickle the result as well

              q"""
               val codecInput = codec[$TupleNCons[..$paramTypes]]
               val codecResult = codec[$out]
               """
            }

          def runImplementation =
            if (argList.isEmpty)
              q"algebra.$name"
            else
              q"algebra.$name(...$argList)"

          val invocation =
            q"""
              ..$argsTerm
              for {
                  args  <- Task.fromTry(codecInput.decodeValue(arguments).toTry).mapError(errorHandler)
                  result <- $runImplementation
                  vector <- Task.fromTry(codecResult.encode(result).toTry).mapError(errorHandler)
              } yield vector
              """

          q"""
             if (hint == $hintToUse)  { $invocation } else $acc"""
      }
    }

    q""" new StemProtocol[$algebra, $state, $event, $reject] {
            import scodec.bits.BitVector
            import boopickle.Default._
            import stem.communication.macros.BoopickleCodec._
            import stem.data.Invocation
            import zio._
            import stem.data.AlgebraCombinators

             private val mainCodec = codec[(String, BitVector)]
             val client: (BitVector => IO[$reject, BitVector], Throwable => $reject) => $algebra =
               (commFn: BitVector => IO[$reject, BitVector], errorHandler: Throwable => $reject) =>
                 new $algebra { ..$stubbedMethods }

             val server: ($algebra, Throwable => $reject) => Invocation[$state, $event, $reject] =
               (algebra: $algebra, errorHandler: Throwable => $reject) =>
                 new Invocation[$state, $event, $reject] {
                   private def buildVectorFromHint(hint: String, arguments: BitVector): ZIO[Has[AlgebraCombinators[$state, $event, $reject]], $reject, BitVector] = { $serverHintBitVectorFunction }

                   override def call(message: BitVector): ZIO[Has[AlgebraCombinators[$state, $event, $reject]], $reject, BitVector] = {
                     // for each method extract the name, it could be a sequence number for the method
                       // according to the hint, extract the arguments
                       for {
                         element <- Task.fromTry(mainCodec.decodeValue(message).toTry).mapError(errorHandler)
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
