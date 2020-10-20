package is
package solidninja
package albion

/** Marker typeclass for types that can be displayed as a string. Similar to the `Show` typeclass, but specifically for
  * supporting custom types that should be serialized as a string
  */
trait DisplayAsString[T] {
  def fromString(s: String): Either[Throwable, T]
  def toString(t: T): String
}

object DisplayAsString {
  def apply[T](from: String => Either[Throwable, T], to: T => String): DisplayAsString[T] = new DisplayAsString[T] {
    override def fromString(s: String): Either[Throwable, T] = from(s)
    override def toString(t: T): String = to(t)
  }
}
