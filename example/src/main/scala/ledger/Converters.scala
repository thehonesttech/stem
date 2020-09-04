package ledger

import ledger.eventsourcing.events.events

object Converters {
  private def toLedgerBigDecimalValue(bigDecimal: BigDecimal): events.BigDecimal =
    ledger.eventsourcing.events.events.BigDecimal(bigDecimal.longValue, bigDecimal.scale)

  def toLedgerBigDecimal(bigDecimal: BigDecimal): Option[events.BigDecimal] =
    Some(toLedgerBigDecimalValue(bigDecimal))

  def fromLedgerBigDecimal(bigDecimal: Option[events.BigDecimal]): BigDecimal = {
    bigDecimal.map(el => BigDecimal(el.unscaledValue, el.scale)).getOrElse(BigDecimal(0))
  }

  object Ops {

    implicit def from(bigDecimal: BigDecimal): Option[events.BigDecimal] = toLedgerBigDecimal(bigDecimal)
    implicit def to(bigDecimal: Option[events.BigDecimal]):BigDecimal = fromLedgerBigDecimal(bigDecimal)
  }

}
