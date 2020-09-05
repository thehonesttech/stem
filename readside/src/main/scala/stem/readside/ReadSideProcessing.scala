package stem.readside

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

import akka.actor.ActorSystem
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings}
import akka.pattern.{BackoffOpts, BackoffSupervisor, ask}
import akka.util.Timeout
import stem.readside.ReadSideProcessing.{KillSwitch, _}
import stem.readside.ReadSideWorkerActor.KeepRunning
import zio.{Runtime, Task, ZEnv, ZIO, ZLayer}

import scala.concurrent.duration.{FiniteDuration, _}

trait ReadSideProcessing {

  def start(name: String, processes: List[Process])(implicit runtime: Runtime[ZEnv]): Task[KillSwitch]
}

final class ActorReadSideProcessing private (system: ActorSystem, settings: ReadSideSettings) extends ReadSideProcessing {

  /**
    * Starts `processes` distributed over underlying akka cluster.
    *
    * @param name      - type name of underlying cluster sharding
    * @param processes - list of processes to distribute
    *
    */
  def start(name: String, processes: List[Process])(
    implicit runtime: Runtime[ZEnv]
  ): Task[KillSwitch] =
    Task {
      val opts = BackoffOpts
        .onFailure(
          ReadSideWorkerActor.props(processes, name),
          "worker",
          settings.minBackoff,
          settings.maxBackoff,
          settings.randomFactor
        )

      val props = BackoffSupervisor.props(opts)

      val region = ClusterSharding(system).start(
        typeName = name,
        entityProps = props,
        settings = settings.clusterShardingSettings,
        extractEntityId = {
          case c @ KeepRunning(workerId) => (workerId.toString, c)
        },
        extractShardId = {
          case KeepRunning(workerId) => (workerId % settings.numberOfShards).toString
          case other                 => throw new IllegalArgumentException(s"Unexpected message [$other]")
        }
      )

      val regionSupervisor = system.actorOf(
        ReadSideSupervisor
          .props(processes.size, region, settings.heartbeatInterval),
        "DistributedProcessingSupervisor-" + URLEncoder
          .encode(name, StandardCharsets.UTF_8.name())
      )
      implicit val timeout = Timeout(settings.shutdownTimeout)
      KillSwitch {
        Task.fromFuture(ec => regionSupervisor ? ReadSideSupervisor.GracefulShutdown).unit
      }
    }
}

object ReadSideProcessing {

  final case class KillSwitch(shutdown: Task[Unit]) extends AnyVal

  final case class RunningProcess(watchTermination: Task[Unit], shutdown: Task[Unit])

  final case class Process(run: Task[RunningProcess]) extends AnyVal

  val live = ZLayer.fromServices { (actorSystem: ActorSystem, readSideSettings: ReadSideSettings) =>
    ActorReadSideProcessing(actorSystem, readSideSettings)
  }

  val memory = ZLayer.succeed {
    new ReadSideProcessing {
      def start(name: String, processes: List[Process])(implicit runtime: Runtime[ZEnv]): Task[KillSwitch] = {
        for {
          tasksToShutdown <- ZIO.foreach(processes)(process => process.run)
        } yield KillSwitch(ZIO.foreach(tasksToShutdown)(_.shutdown).as())
      }
    }
  }

}

object ActorReadSideProcessing {
  def apply(system: ActorSystem, settings: ReadSideSettings): ReadSideProcessing = new ActorReadSideProcessing(system, settings)

}

final case class ReadSideSettings(
  minBackoff: FiniteDuration,
  maxBackoff: FiniteDuration,
  randomFactor: Double,
  shutdownTimeout: FiniteDuration,
  numberOfShards: Int,
  heartbeatInterval: FiniteDuration,
  clusterShardingSettings: ClusterShardingSettings
)

object ReadSideSettings {
  def default(clusterShardingSettings: ClusterShardingSettings): ReadSideSettings =
    ReadSideSettings(
      minBackoff = 3.seconds,
      maxBackoff = 10.seconds,
      randomFactor = 0.2,
      shutdownTimeout = 10.seconds,
      numberOfShards = 100,
      heartbeatInterval = 2.seconds,
      clusterShardingSettings = clusterShardingSettings
    )

  def default(system: ActorSystem): ReadSideSettings =
    default(ClusterShardingSettings(system))
}
