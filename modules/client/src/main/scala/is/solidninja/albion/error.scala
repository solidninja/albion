package is
package solidninja
package albion

final case class UnableToDerive(message: String) extends RuntimeException(message)

sealed abstract class DecodingError(message: String, cause: Throwable = null) extends RuntimeException(message, cause)

final case class MissingRecordSchema() extends DecodingError("Missing schema from row")
final case class TypeMismatch(expected: String, got: String)
    extends DecodingError(s"Mismatching types: expected a $expected, got $got")
final case class UnexpectedNullable(expected: String)
    extends DecodingError(s"Expected non-nullable type $expected, but got null instead")
final case class FieldNotPresent(name: String, cause: Throwable = null)
    extends DecodingError(s"Field '$name' was not present in record", cause)
