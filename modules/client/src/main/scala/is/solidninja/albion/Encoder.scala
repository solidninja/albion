package is
package solidninja
package albion

import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}
import java.util.{Base64, UUID}

import cats.Contravariant
import cats.syntax.contravariant._
import scala.jdk.CollectionConverters._

import magnolia._

/** Internal representation for building the insertable BigQuery Map
  */
private[albion] sealed trait MapRepr {
  def value: AnyRef
}

private[albion] object MapRepr {
  final case class Value(value: AnyRef) extends MapRepr
  final case class ValueMap(value: java.util.Map[String, AnyRef]) extends MapRepr
}

/** An Encoder that takes a scala type `T` and converts it into a representation that is insertable into a BigQuery
  * table
  */
trait Encoder[T] {
  private[albion] def repr(t: T): MapRepr

  final def encode(t: T): java.util.Map[String, AnyRef] = repr(t) match {
    case MapRepr.Value(v)    => throw UnableToDerive(s"Unable to derive map encoder for primitive value $v")
    case MapRepr.ValueMap(m) => m
  }
}

object Encoder {
  type Typeclass[T] = Encoder[T]

  // BigQuery only supports microsecond precision, and these formats should only be used for encoding
  private val InstantFormat = new DateTimeFormatterBuilder().appendInstant(6).toFormatter()
  private val DateFormat = DateTimeFormatter.ofPattern("uuuu-MM-dd")
  private val DateTimeFormat = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSSSS")
  private val TimeFormat = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS")

  def apply[T](implicit ev: Typeclass[T]): Typeclass[T] = ev

  implicit val contravariantEncoder: Contravariant[Typeclass] = new Contravariant[Typeclass] {
    override def contramap[A, B](fa: Typeclass[A])(f: B => A): Typeclass[B] = instance[B](b => fa.repr(f(b)))
  }

  // passthrough encoder
  implicit val encodePassthrough: Encoder[java.util.Map[String, AnyRef]] = instance(MapRepr.ValueMap)

  implicit val encodeBigDecimal: Encoder[BigDecimal] = primitive[java.math.BigDecimal].contramap(_.bigDecimal)
  implicit val encodeBoolean: Encoder[Boolean] = primitive[java.lang.Boolean].contramap[Boolean](b => b)
  // see [[com.google.cloud.bigquery.InsertAllRequest.RowToInsert]]
  implicit val encodeByteArray: Encoder[Array[Byte]] =
    primitive[String].contramap[Array[Byte]](bytes => Base64.getEncoder.encodeToString(bytes))
  implicit val encodeDouble: Encoder[Double] = primitive[java.lang.Double].contramap[Double](d => d)
  implicit val encodeFloat: Encoder[Float] = primitive[java.lang.Double].contramap[Float](f => f.toDouble)
  implicit val encodeInt: Encoder[Int] = primitive[java.lang.Long].contramap[Int](f => f.toLong)
  implicit val encodeInstant: Encoder[Instant] = primitive[String].contramap[Instant](InstantFormat.format(_))
  implicit val encodeLocalDate: Encoder[LocalDate] = primitive[String].contramap(_.format(DateFormat))
  implicit val encodeLocalDateTime: Encoder[LocalDateTime] = primitive[String].contramap(_.format(DateTimeFormat))
  implicit val encodeLocalTime: Encoder[LocalTime] = primitive[String].contramap(_.format(TimeFormat))
  implicit val encodeLong: Encoder[Long] = primitive[java.lang.Long].contramap[Long](l => l)
  implicit val encodeString: Encoder[String] = primitive[String]
  implicit val encodeUUID: Encoder[UUID] = primitive[String].contramap(_.toString)

  implicit def encodeOption[T](implicit ev: Encoder[T]): Encoder[Option[T]] =
    instance[Option[T]](_.fold[MapRepr](MapRepr.Value(null))(ev.repr))

  // sequence encoders
  implicit def encodeArray[T](implicit ev: Encoder[T]): Encoder[Array[T]] =
    instance[Array[T]](s => MapRepr.Value(s.map(t => ev.repr(t).value).toList.asJava))
  implicit def encodeList[T](implicit ev: Encoder[T]): Encoder[List[T]] =
    instance[List[T]](s => MapRepr.Value(s.map(t => ev.repr(t).value).asJava))
  implicit def encodeVector[T](implicit ev: Encoder[T]): Encoder[Vector[T]] =
    instance[Vector[T]](s => MapRepr.Value(s.map(t => ev.repr(t).value).asJava))
  implicit def encodeSeq[T](implicit ev: Encoder[T]): Encoder[Seq[T]] =
    instance[Seq[T]](s => MapRepr.Value(s.map(t => ev.repr(t).value).asJava))

  private def primitive[T <: AnyRef]: Encoder[T] = instance(MapRepr.Value(_))

  private def instance[T](f: T => MapRepr): Encoder[T] = new Encoder[T] {
    override private[albion] def repr(t: T) = f(t)
  }

  def combine[T](caseClass: CaseClass[Typeclass, T])(implicit config: Configuration = Configuration()): Typeclass[T] = {
    if (caseClass.isValueClass) instance[T] { t =>
      val param = caseClass.parameters.head
      param.typeclass.repr(param.dereference(t))
    }
    else {
      instance { t =>
        val fields: Map[String, AnyRef] = caseClass.parameters.map { p =>
          val label = config.transformMemberNames(p.label)
          label -> p.typeclass.repr(p.dereference(t)).value
        }.toMap
        MapRepr.ValueMap(fields.asJava)
      }
    }
  }

  implicit def gen[T]: Typeclass[T] = macro Magnolia.gen[T]
}
