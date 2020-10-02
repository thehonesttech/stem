package stem

object Converters {
  def toCommonBigDecimalValue(bigDecimal: BigDecimal): commons.BigDecimal =
    commons.BigDecimal(bigDecimal.longValue, bigDecimal.scale)

  def fromCommonBigDecimalValue(el: commons.BigDecimal) = BigDecimal(el.unscaledValue, el.scale)

  def toGrpcBigDecimal(bigDecimal: BigDecimal): Option[commons.BigDecimal] =
    Some(toCommonBigDecimalValue(bigDecimal))

  def fromGrpcBigDecimal(bigDecimal: Option[commons.BigDecimal]): BigDecimal = {
    bigDecimal.map(fromCommonBigDecimalValue).getOrElse(BigDecimal(0))
  }

}
