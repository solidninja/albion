package is
package solidninja
package albion

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant, LocalDate, LocalDateTime, LocalTime}
import java.util.{Base64, UUID}

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.Try

import cats.instances.either._
import cats.instances.list._
import cats.syntax.traverse._
import com.google.cloud.bigquery.{
  FieldList => BQFieldList,
  FieldValue => BQFieldValue,
  FieldValueList => BQFieldValueList
}
import magnolia._

import ScalaCompat._

/**
  * Internal representation of the BigQuery encoded content
  */
sealed trait EncodedRepr

private[albion] object EncodedRepr {
  final case class Record private (values: BQFieldValueList) extends EncodedRepr

  object Record {
    def apply(values: BQFieldValueList): Record = {
      if (!values.hasSchema) throw MissingRecordSchema()
      new Record(values)
    }
  }

  final case class Field(value: BQFieldValue) extends EncodedRepr

  final case class Repeated private (values: List[EncodedRepr]) extends EncodedRepr

  object Repeated {
    def apply(values: List[BQFieldValue], _ev: Dummy): Repeated = {
      val _ = _ev // ugly hack
      new Repeated(
        values.map(
          fv =>
            fv.getAttribute match {
              case BQFieldValue.Attribute.PRIMITIVE => Field(fv)
              case BQFieldValue.Attribute.RECORD    => Record(fv.getRecordValue)
              case BQFieldValue.Attribute.REPEATED =>
                throw new IllegalStateException("BUG: Found nested REPEATED record, this should not be possible")
            }
        )
      )
    }
  }
}

/**
  * Transformer from the BigQuery internal result representation into a type `A`
  */
trait Decoder[A] { self =>
  def decode(repr: EncodedRepr): Either[DecodingError, A]

  final def map[B](f: A => Either[DecodingError, B]): Decoder[B] = new Decoder[B] {
    override def decode(repr: EncodedRepr): Either[DecodingError, B] = self.decode(repr).flatMap(f)
  }
}

object Decoder {
  type Typeclass[T] = Decoder[T]

  private val Microseconds: Long = 1000000L

  def apply[T](implicit ev: Typeclass[T]): Typeclass[T] = ev

  // allow to use raw library representation
  implicit val idDecoder: Decoder[BQFieldValueList] = instance {
    case EncodedRepr.Record(fieldValues) => Right(fieldValues)
    case EncodedRepr.Repeated(_)         => Left(TypeMismatch("record", "repeated"))
    case EncodedRepr.Field(_)            => Left(TypeMismatch("record", "field"))
  }

  implicit val byteArrayDecoder: Decoder[Array[Byte]] = stringlyTyped(fromB64Bytes)
  implicit val booleanDecoder: Decoder[Boolean] = stringlyTyped(fromBoolean)
  implicit val bigdecimalDecoder: Decoder[BigDecimal] = stringlyTyped(fromBigDecimal)
  implicit val doubleDecoder: Decoder[Double] = stringlyTyped(fromDouble)
  implicit val floatDecoder: Decoder[Float] = stringlyTyped(fromFloat)
  implicit val intDecoder: Decoder[Int] = stringlyTyped(fromInt)
  implicit val instantDecoder: Decoder[Instant] = stringlyTyped(fromTimestamp)
  implicit val localDateDecoder: Decoder[LocalDate] = stringlyTyped(fromLocalDate)
  implicit val localDateTimeDecoder: Decoder[LocalDateTime] = stringlyTyped(fromLocalDateTime)
  implicit val localTimeDecoder: Decoder[LocalTime] = stringlyTyped(fromLocalTime)
  implicit val longDecoder: Decoder[Long] = stringlyTyped(fromLong)
  implicit val stringDecoder: Decoder[String] = stringlyTyped(Right(_))
  implicit val uuidDecoder: Decoder[UUID] = stringlyTyped(fromUuid)

  implicit def optionDecoder[A](implicit ev: Decoder[A]): Decoder[Option[A]] = instance {
    case f @ EncodedRepr.Field(fieldValue) => if (fieldValue.isNull) Right(None) else ev.decode(f).map(Some(_))
    case r @ EncodedRepr.Record(fieldValues) =>
      if (fieldValues.isEmpty || fieldValues.row.asScala.forall(_.isNull)) Right(None) else ev.decode(r).map(Some(_))
    case r @ EncodedRepr.Repeated(reprs) => if (reprs.isEmpty) Right(None) else ev.decode(r).map(Some(_))
  }

  implicit def decodeArray[A: Decoder: ClassTag]: Decoder[Array[A]] = collection[A].map(l => Right(l.toArray))
  implicit def decodeList[A: Decoder]: Decoder[List[A]] = collection[A]
  implicit def decodeVector[A: Decoder]: Decoder[Vector[A]] = collection[A].map(l => Right(l.toVector))
  implicit def decodeSeq[A: Decoder]: Decoder[Seq[A]] = collection[A].map(l => Right(l.toSeq))

  private def collection[A](implicit decoder: Decoder[A]): Decoder[List[A]] = instance[List[A]] {
    case EncodedRepr.Repeated(values) => values.traverse(decoder.decode)
    case got                          => Left(TypeMismatch("repeated", s"$got"))
  }

  private def stringlyTyped[A: ClassTag](f: String => Either[DecodingError, A]): Decoder[A] =
    primitiveDecoder[A](field => f(field.value.getStringValue))

  private def primitiveDecoder[A: ClassTag](f: EncodedRepr.Field => Either[DecodingError, A]): Decoder[A] =
    instance[A] {
      case field: EncodedRepr.Field if field.value.getAttribute == BQFieldValue.Attribute.PRIMITIVE => f(field)
      case other                                                                                    => Left(TypeMismatch(implicitly[ClassTag[A]].runtimeClass.getName, other.toString))
    }

  /**
    * @see com.google.cloud.bigquery.FieldValue.getTimestampValue for explanation
    */
  private def fromTimestamp(s: String): Either[DecodingError, Instant] =
    s.toDoubleOption
      .map { d =>
        val micros = (d * Microseconds).longValue
        Instant.EPOCH.plus(Duration.of(micros, ChronoUnit.MICROS))
      }
      .toRight(TypeMismatch("timestamp in format <seconds since epoch>.<microseconds>", s))

  private def fromLocalDate(s: String): Either[DecodingError, LocalDate] =
    Try(LocalDate.parse(s)).toEither.left.map(ex => TypeMismatch("local date", s"`$s`: ${ex.getMessage}"))

  private def fromLocalDateTime(s: String): Either[DecodingError, LocalDateTime] =
    Try(LocalDateTime.parse(s)).toEither.left
      .map(ex => TypeMismatch("local date time", s"`$s`: ${ex.getMessage}"))

  private def fromLocalTime(s: String): Either[DecodingError, LocalTime] =
    Try(LocalTime.parse(s)).toEither.left.map(ex => TypeMismatch("local time", s"`$s`: ${ex.getMessage}"))

  private def fromB64Bytes(s: String): Either[DecodingError, Array[Byte]] =
    Try(Base64.getDecoder.decode(s)).toEither.left.map(ex => TypeMismatch("byte[]", ex.getMessage))

  private def fromBoolean(s: String): Either[DecodingError, Boolean] =
    s.toBooleanOption.toRight(TypeMismatch("boolean", s))

  private def fromBigDecimal(s: String): Either[DecodingError, BigDecimal] =
    Try(BigDecimal(s)).toEither.left.map(_ => TypeMismatch("BigDecimal", s))

  private def fromDouble(s: String): Either[DecodingError, Double] =
    s.toDoubleOption.toRight(TypeMismatch("double", s))

  private def fromFloat(s: String): Either[DecodingError, Float] =
    s.toFloatOption.toRight(TypeMismatch("float", s))

  private def fromLong(s: String): Either[DecodingError, Long] =
    s.toLongOption.toRight(TypeMismatch("long", s))

  private def fromInt(s: String): Either[DecodingError, Int] =
    s.toIntOption.toRight(TypeMismatch("int", s))

  private def fromUuid(s: String): Either[DecodingError, UUID] =
    Try(UUID.fromString(s)).toEither.left.map(ex => TypeMismatch("uuid", ex.getMessage))

  private def instance[A](f: EncodedRepr => Either[DecodingError, A]): Decoder[A] = new Decoder[A] {
    override def decode(repr: EncodedRepr): Either[DecodingError, A] = f(repr)
  }

  private def decodeRecord[A](caseClass: CaseClass[Typeclass, A], row: BQFieldValueList)(
      implicit configuration: Configuration
  ): Either[DecodingError, A] =
    caseClass.constructMonadic { p =>
      val label = configuration.transformMemberNames(p.label)
      for {
        raw <- Try(row.get(label)).toEither.left.map(FieldNotPresent(label, _))
        repr = toRepr(row, label, raw)
        value <- p.typeclass.decode(repr)
      } yield value
    }

  private def decodeSingleRecord[A](
      caseClass: CaseClass[Typeclass, A],
      value: BQFieldValue
  ): Either[DecodingError, A] = {
    caseClass.constructMonadic { p =>
      val repr = value.getAttribute match {
        case BQFieldValue.Attribute.PRIMITIVE => EncodedRepr.Field(value)
        case BQFieldValue.Attribute.REPEATED  => EncodedRepr.Repeated(value.getRepeatedValue.asScala.toList, ())
        case BQFieldValue.Attribute.RECORD    => throw TypeMismatch("single-field record", "full-fledged record")
      }
      p.typeclass.decode(repr)
    }
  }

  private def toRepr(parent: BQFieldValueList, label: String, field: BQFieldValue): EncodedRepr =
    field.getAttribute match {
      case BQFieldValue.Attribute.PRIMITIVE => EncodedRepr.Field(field)
      case BQFieldValue.Attribute.RECORD =>
        if (field.getRecordValue.hasSchema) {
          EncodedRepr.Record(field.getRecordValue)
        } else {
          // NOTE: the library seems to return a null schema in the nested FieldValueList, so construct it manually
          val schema = parent.schema.get(label)
          EncodedRepr.Record(BQFieldValueList.of(field.getRecordValue.row, schema.getSubFields))
        }
      case BQFieldValue.Attribute.REPEATED =>
        // NOTE: same thing as above, need to recover record schemas in repeated list
        val schema = parent.schema.get(label)
        val values =
          if (field.isNull) Nil
          else
            field.getRepeatedValue.asScala.map {
              case record if record.getAttribute == BQFieldValue.Attribute.RECORD =>
                BQFieldValue.of(record.getAttribute, BQFieldValueList.of(record.getRecordValue, schema.getSubFields))
              case other => other
            }.toList
        EncodedRepr.Repeated(values, ())
    }

  def combine[T](caseClass: CaseClass[Typeclass, T])(implicit config: Configuration = Configuration()): Typeclass[T] = {
    if (caseClass.isObject)
      throw UnableToDerive(s"Singleton objects are not supported for derivation, tried: ${caseClass.typeName.full}")

    instance {
      case EncodedRepr.Record(row) => decodeRecord(caseClass, row)
      case EncodedRepr.Field(record)
          if caseClass.parameters.size > 1 && record.getAttribute == BQFieldValue.Attribute.RECORD =>
        decodeRecord(caseClass, record.getRecordValue)
      case EncodedRepr.Field(value) if caseClass.isValueClass || caseClass.parameters.size <= 1 =>
        decodeSingleRecord(caseClass, value)
      case EncodedRepr.Repeated(_) => throw TypeMismatch("record or field", "repeated")
    }
  }

  @debug
  implicit def gen[T]: Typeclass[T] = macro Magnolia.gen[T]

  // accessors for private schema and row fields
  private[this] val fieldValueSchemaAccessor: BQFieldValueList => BQFieldList = {
    val cls = classOf[BQFieldValueList]
    val field = cls.getDeclaredField("schema")
    field.setAccessible(true)
    fieldValueList => field.get(fieldValueList).asInstanceOf[BQFieldList]
  }

  private[this] val fieldValueRowAccessor: BQFieldValueList => java.util.List[BQFieldValue] = {
    val cls = classOf[BQFieldValueList]
    val field = cls.getDeclaredField("row")
    field.setAccessible(true)
    fieldValueList => field.get(fieldValueList).asInstanceOf[java.util.List[BQFieldValue]]
  }

  private[this] final implicit class FieldValueListSyntax(val fieldValueList: BQFieldValueList) extends AnyVal {
    def schema: BQFieldList = fieldValueSchemaAccessor(fieldValueList)
    def row: java.util.List[BQFieldValue] = fieldValueRowAccessor(fieldValueList)
  }
}
