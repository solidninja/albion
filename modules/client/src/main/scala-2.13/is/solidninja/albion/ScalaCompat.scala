package is
package solidninja
package albion

// IterableOnce != TraversableOnce in 2.12
final case class IterableResult[A](rowCount: Long, iterator: Iterator[A]) extends IterableOnce[A] {
  override def knownSize: Int = rowCount.toInt
}

// compatibility module for 2.12 since 2.12 compiler does not have -Yimports: and collection changes
private[albion] object ScalaCompat {
  // unused import :/
  type Dummy = Unit
}
