package stem.idempotency

import zio.Task

trait IdempotencyKeyStore {

  def isAlreadyProcessed[T](context: Class[T], idempotencyValue: WithIdempotencyKey): Task[Boolean]

  def setIdempotencyKey[T](context: Class[T], idempotencyValue: WithIdempotencyKey): Task[Unit]

}

object NoIdempotencyKey extends IdempotencyKeyStore {
  override def isAlreadyProcessed[T](context: Class[T], idempotencyValue: WithIdempotencyKey): Task[Boolean] = Task.succeed(false)

  override def setIdempotencyKey[T](context: Class[T], idempotencyValue: WithIdempotencyKey): Task[Unit] = Task.unit
}
