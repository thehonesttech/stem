package stem.idempotency

trait WithIdempotencyKey {
  def idempotencyKey: IdempotencyKey
}

case class IdempotencyKey(value: String) extends AnyVal
