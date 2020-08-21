package stem.communication.macros

import stem.data.StemProtocol

import scala.language.experimental.macros

object RpcMacro {

  def derive[Algebra, State, Event, Reject]: StemProtocol[Algebra, State, Event, Reject] = macro DeriveMacros.derive[Algebra, State, Event, Reject]

}
