package stem

import scalapb.TypeMapper

package object commons {

  implicit val bigDecimalConversions =
    TypeMapper[commons.BigDecimal, scala.math.BigDecimal](in => Converters.fromCommonBigDecimalValue(in))(Converters.toCommonBigDecimalValue)

}
