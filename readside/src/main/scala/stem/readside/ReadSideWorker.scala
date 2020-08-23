package stem.readside

import akka.actor.{Actor, ActorLogging, Props, Status}
import stem.readside.ReadSideProcessing.RunningProcess
import stem.readside.ReadSideWorker.KeepRunning
import stem.readside.serialization.Message
import zio.{Runtime, Task, ZEnv}
import ReadSideProcessing.Process
import akka.pattern._


object ReadSideWorker {
  def props(processWithId: Int => Process, processName: String)(implicit runtime: Runtime[ZEnv]): Props =
    Props(new ReadSideWorker(processWithId, processName))

  final case class KeepRunning(workerId: Int) extends Message

}


final class ReadSideWorker(
                            processFor: Int => Process,
                            processName: String
                          )(implicit val runtime: Runtime[ZEnv]) extends Actor with ActorLogging {

  import context.dispatcher

  case class ProcessStarted(process: RunningProcess)

  case object ProcessTerminated

  var killSwitch: Option[Task[Unit]] = None

  override def postStop(): Unit =
    killSwitch.foreach(el => runtime.unsafeRun(el))

  def receive: Receive = {
    case KeepRunning(workerId) =>
      log.info("[{}] Starting process {}", workerId, processName)
      runtime.unsafeRunToFuture(processFor(workerId).run
        .map(ProcessStarted)) pipeTo self
      context.become {
        case ProcessStarted(RunningProcess(watchTermination, terminate)) =>
          log.info("[{}] Process started {}", workerId, processName)
          killSwitch = Some(terminate)
          runtime.unsafeRunToFuture(watchTermination.map(_ => ProcessTerminated)) pipeTo self
          context.become {
            case Status.Failure(e) =>
              log.error(e, "Process failed {}", processName)
              throw e
            case ProcessTerminated =>
              log.error("Process terminated {}", processName)
              throw new IllegalStateException(s"Process terminated $processName")
          }
        case Status.Failure(e) =>
          log.error(e, "Process failed to start {}", processName)
          throw e
        case KeepRunning(_) => ()
      }
  }
}
