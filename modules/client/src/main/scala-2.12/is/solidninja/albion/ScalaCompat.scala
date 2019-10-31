package is
package solidninja
package albion

import scala.util.Try

final case class IterableResult[A](rowCount: Long, iterator: Iterator[A]) extends Iterable[A] {
  override def hasDefiniteSize: Boolean = true
  override def size: Int = rowCount.toInt
}

private[albion] object ScalaCompat {
  type Dummy = Unit

  // These are not available in 2.12 and we don't want to reimplement scala.collections.StringParsers
  implicit final class Scala213StringOps(val s: String) extends AnyVal {

    def toBooleanOption: Option[Boolean] =
      Try(java.lang.Boolean.parseBoolean(s)).toOption

    def toDoubleOption: Option[Double] =
      Try(java.lang.Double.parseDouble(s)).toOption

    def toFloatOption: Option[Float] =
      Try(java.lang.Float.parseFloat(s)).toOption

    def toIntOption: Option[Int] =
      Try(java.lang.Integer.parseInt(s)).toOption

    def toLongOption: Option[Long] =
      Try(java.lang.Long.parseLong(s)).toOption

  }
}
