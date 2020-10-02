package stem

import scalapb.TypeMapper

package object commons {
  private def toCommonBigDecimalValue(bigDecimal: scala.math.BigDecimal): commons.BigDecimal =
    commons.BigDecimal(bigDecimal.longValue, bigDecimal.scale)

  private def fromCommonBigDecimalValue(el: commons.BigDecimal): scala.math.BigDecimal = scala.math.BigDecimal(el.unscaledValue, el.scale)

  implicit val bigDecimalConversions =
    TypeMapper[commons.BigDecimal, scala.math.BigDecimal](in => fromCommonBigDecimalValue(in))(toCommonBigDecimalValue)

}
