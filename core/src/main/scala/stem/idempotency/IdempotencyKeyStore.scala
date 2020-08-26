package stem.idempotency

import zio.{Tag, Task}

trait IdempotencyKeyStore {

  def isAlreadyProcessed[T: Tag](idempotencyValue: WithIdempotencyKey): Task[Boolean]

  def setIdempotencyKey[T: Tag](idempotencyValue: WithIdempotencyKey): Task[Unit]

}

object NoIdempotencyKey extends IdempotencyKeyStore {
  override def isAlreadyProcessed[T: Tag](idempotencyValue: WithIdempotencyKey): Task[Boolean] = Task.succeed(false)

  override def setIdempotencyKey[T: Tag](idempotencyValue: WithIdempotencyKey): Task[Unit] = Task.unit
}
