package is
package solidninja
package albion

import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}
import java.util.UUID

import magnolia._
import com.google.cloud.bigquery.{Field => BQField}

/**
  * Internal representation of the schema while it is being derived. Necessary because a primitive field does not have
  *  a name until we get to a case class, and using a blank name in TableField would feel too hacky
  */
private[albion] sealed trait SchemaRepr {
  def mode: BQField.Mode
}

private[albion] object SchemaRepr {
  final case class Primitive(`type`: SQLType, mode: BQField.Mode) extends SchemaRepr
  final case class Struct(fields: List[TableField], mode: BQField.Mode = BQField.Mode.REQUIRED) extends SchemaRepr

  final implicit class SchemaReprSyntax(val repr: SchemaRepr) extends AnyVal {
    def setMode(mode: BQField.Mode): SchemaRepr = repr match {
      case p: Primitive => p.copy(mode = mode)
      case s: Struct    => s.copy(mode = mode)
    }

    def toTableField(name: String, description: Option[String] = None): TableField = repr match {
      case p: Primitive => TableField(name, p.mode).description(description).primitive(p.`type`)
      case s: Struct    => TableField(name, s.mode).description(description).struct(s.fields)
    }
  }
}

/**
  * A [[SchemaFor]] generates a BigQuery table schema for a record type.
  */
trait SchemaFor[T] {
  private[albion] def repr: SchemaRepr

  final def schema: TableSchema = repr match {
    case p: SchemaRepr.Primitive =>
      throw UnableToDerive(s"Cannot derive a table schema for a primitive type $p")
    case s: SchemaRepr.Struct => TableSchema(s.fields)
  }
}

object SchemaFor {
  type Typeclass[T] = SchemaFor[T]

  def apply[T](implicit ev: Typeclass[T]): Typeclass[T] = ev

  private def instance[T](theRepr: SchemaRepr): SchemaFor[T] = new SchemaFor[T] {
    override private[albion] val repr = theRepr
  }

  implicit val bigDecimalSchema: SchemaFor[BigDecimal] = primitive(SQLType.numeric)
  implicit val jBigDecimalSchema: SchemaFor[java.math.BigDecimal] = primitive(SQLType.numeric)
  implicit val booleanSchema: SchemaFor[Boolean] = primitive(SQLType.bool)
  implicit val byteArraySchema: SchemaFor[Array[Byte]] = primitive(SQLType.bytes)
  implicit val doubleSchema: SchemaFor[Double] = primitive(SQLType.double)
  implicit val floatSchema: SchemaFor[Float] = primitive(SQLType.double)
  implicit val instantSchema: SchemaFor[Instant] = primitive(SQLType.timestamp)
  implicit val intSchema: SchemaFor[Int] = primitive(SQLType.long)
  implicit val localDateSchema: SchemaFor[LocalDate] = primitive(SQLType.date)
  implicit val localDateTimeSchema: SchemaFor[LocalDateTime] = primitive(SQLType.datetime)
  implicit val localTimeSchema: SchemaFor[LocalTime] = primitive(SQLType.time)
  implicit val longSchema: SchemaFor[Long] = primitive(SQLType.long)
  implicit val stringSchema: SchemaFor[String] = primitive(SQLType.string)
  implicit val uuidSchema: SchemaFor[UUID] = primitive(SQLType.string)

  // TODO: support geography
  // TODO: support enums

  // it's not safe to mode to nullable when the mode is already repeated
  implicit def optSchema[T](implicit ev: SchemaFor[T]): SchemaFor[Option[T]] = ev.repr.mode match {
    case BQField.Mode.REPEATED => instance(ev.repr) // no-op, repeated is always nullable
    case _                     => instance(ev.repr.setMode(BQField.Mode.NULLABLE))
  }

  implicit def arraySchema[T: SchemaFor]: SchemaFor[Array[T]] = repeated[Array, T]
  implicit def listSchema[T: SchemaFor]: SchemaFor[List[T]] = repeated[List, T]
  implicit def vectorSchema[T: SchemaFor]: SchemaFor[Vector[T]] = repeated[Vector, T]
  implicit def seqSchema[T: SchemaFor]: SchemaFor[Seq[T]] = repeated[Seq, T]

  implicit def forCustomDisplayAsString[T: DisplayAsString]: SchemaFor[T] = primitive(SQLType.string)

  private def primitive[T](`type`: SQLType, mode: BQField.Mode = BQField.Mode.REQUIRED): SchemaFor[T] =
    new SchemaFor[T] {
      override private[albion] val repr = SchemaRepr.Primitive(`type`, mode)
    }

  private def repeated[C[_], T](implicit ev: SchemaFor[T]): SchemaFor[C[T]] = ev.repr.mode match {
    // it's also not safe to have nested repeated modes
    case BQField.Mode.REPEATED =>
      throw UnableToDerive(s"Cannot make something repeated twice, already have ${ev.repr}")
    case _ => instance(ev.repr.setMode(BQField.Mode.REPEATED))
  }

  def combine[T](caseClass: CaseClass[Typeclass, T])(implicit config: Configuration = Configuration()): Typeclass[T] = {
    val fields = caseClass.parameters.map { p =>
      val newLabel = config.transformMemberNames(p.label)
      p.typeclass.repr.toTableField(newLabel)
    }

    instance(SchemaRepr.Struct(fields.toList))
  }

  def dispatch[T](sealedTrait: SealedTrait[Typeclass, T]): Typeclass[T] =
    throw UnableToDerive(
      s"Unable to derive schema for a sealed trait ${sealedTrait.typeName} because BigQuery has no union support"
    )

  @debug
  implicit def gen[T]: Typeclass[T] = macro Magnolia.gen[T]
}
