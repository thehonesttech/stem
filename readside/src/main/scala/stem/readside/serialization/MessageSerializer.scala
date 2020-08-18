package stem.readside.serialization

import akka.actor.ExtendedActorSystem
import akka.serialization.{BaseSerializer, SerializerWithStringManifest}
import stem.readside.ReadSideWorker.KeepRunning

class MessageSerializer(val system: ExtendedActorSystem) extends SerializerWithStringManifest with BaseSerializer {
  val KeepRunningManifest = "A"
  override def manifest(o: AnyRef): String = o match {
    case KeepRunning(_) => KeepRunningManifest
    case x              => throw new IllegalArgumentException(s"Serialization of [$x] is not supported")
  }

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case KeepRunning(workerId) => msg.readside.KeepRunning(workerId).toByteArray
    case x                     => throw new IllegalArgumentException(s"Serialization of [$x] is not supported")
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
    manifest match {
      case KeepRunningManifest =>
        KeepRunning(msg.readside.KeepRunning.parseFrom(bytes).workerId)
      case other => throw new IllegalArgumentException(s"Unknown manifest [$other]")
    }
}
