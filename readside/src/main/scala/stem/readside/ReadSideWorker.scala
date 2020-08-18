package stem.readside

import stem.readside.serialization.Message

object ReadSideWorker {

  final case class KeepRunning(workerId: Int) extends Message
}
