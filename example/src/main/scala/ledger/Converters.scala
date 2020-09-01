package ledger

import ledger.eventsourcing.events.events

object Converters {
  private def toLedgerBigDecimalValue(bigDecimal: BigDecimal): events.BigDecimal =
    ledger.eventsourcing.events.events.BigDecimal(bigDecimal.scale, bigDecimal.precision)

  def toLedgerBigDecimal(bigDecimal: BigDecimal): Option[events.BigDecimal] =
    Some(toLedgerBigDecimalValue(bigDecimal))

}
