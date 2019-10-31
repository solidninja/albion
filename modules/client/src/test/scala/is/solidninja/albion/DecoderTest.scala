package is
package solidninja
package albion

import java.time._
import java.util.{Base64, UUID}

import scala.jdk.CollectionConverters._

import com.google.cloud.bigquery.{FieldValue, FieldValueList, Schema => BQSchema}
import com.softwaremill.diffx.{Derived, Diff}
import minitest._

object DecoderTest extends SimpleTestSuite {

  implicit val diffByteArray: Derived[Diff[Array[Byte]]] = Derived(
    Diff[String].contramap(Base64.getEncoder.encodeToString)
  )

  final case class PrimitiveShowcase(
      a: Array[Byte],
      b: Boolean,
      c: Double,
      d: Int,
      e: Instant,
      f: LocalDate,
      g: LocalDateTime,
      h: Long,
      i: String,
      j: UUID,
      k: BigDecimal,
      l: Float,
      m: LocalTime
  )

  test("Decoder for primitives should be derived and produce correct results") {
    val decoder = Decoder[PrimitiveShowcase]

    val bytesHex = "CfkRAp1041vYQVbFY1aIwA=="
    val uuid = "b570d3cd-01fe-43b5-ae99-303824630375"

    val toDecode = FieldValueList.of(
      List(
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, bytesHex),
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "true"),
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.1"),
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "42"),
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1408452095.22"), // == 2014-08-19 07:41:35.220 -05:00
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "9999-12-31"),
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2012-08-12T03:45:30.123456"),
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "9223372036854775807"),
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "hello world"),
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, uuid),
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1238126387123.1234"),
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "42.1"),
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "10:15:30")
      ).asJava,
      (SchemaFor[PrimitiveShowcase].schema: BQSchema).getFields
    )

    val expected = PrimitiveShowcase(
      a = Base64.getDecoder.decode(bytesHex),
      b = true,
      c = 1 / 10.0,
      d = 42,
      e = ZonedDateTime.parse("2014-08-19T07:41:35.220-05:00").toInstant,
      f = LocalDate.of(9999, 12, 31),
      g = LocalDateTime.of(2012, 8, 12, 3, 45, 30, 123456000),
      h = Long.MaxValue,
      i = "hello world",
      j = UUID.fromString(uuid),
      k = BigDecimal("1238126387123.1234"),
      l = 42.1f,
      m = LocalTime.of(10, 15, 30)
    )

    val got = decoder.decode(EncodedRepr.Record(toDecode))
    val diff = Diff[PrimitiveShowcase].apply(expected, got.toTry.get)
    assert(diff.isIdentical, diff.show)
  }

  final case class NestedOptionalInner(a: String, b: Option[Double])

  final case class OptionalShowCase(
      a: Option[String],
      b: Option[Long],
      c: Option[Seq[Boolean]],
      d: Seq[Option[Int]],
      e: Option[NestedOptionalInner],
      f: List[Option[NestedOptionalInner]]
  )

  test("Decoder should be derivable for Option and combinations and work correctly") {
    val decoder = Decoder[OptionalShowCase]

    val expected = OptionalShowCase(
      a = Some("foobar"),
      b = None,
      c = Some(Seq(true, false)),
      d = Nil,
      e = None,
      f = List(Some(NestedOptionalInner("a", None)), Some(NestedOptionalInner("b", Some(42.0))))
    )

    val toDecode = FieldValueList.of(
      List(
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "foobar"),
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, null),
        FieldValue.of(
          FieldValue.Attribute.REPEATED,
          FieldValueList.of(
            List(
              FieldValue.of(FieldValue.Attribute.PRIMITIVE, "true"),
              FieldValue.of(FieldValue.Attribute.PRIMITIVE, "false")
            ).asJava
          )
        ),
        FieldValue.of(FieldValue.Attribute.REPEATED, null),
        FieldValue.of(
          FieldValue.Attribute.RECORD,
          FieldValueList.of(
            List(
              FieldValue.of(FieldValue.Attribute.PRIMITIVE, null),
              FieldValue.of(FieldValue.Attribute.PRIMITIVE, null)
            ).asJava
          )
        ),
        FieldValue.of(
          FieldValue.Attribute.REPEATED,
          FieldValueList.of(
            List(
              FieldValue.of(
                FieldValue.Attribute.RECORD,
                FieldValueList.of(
                  List(
                    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "a"),
                    FieldValue.of(FieldValue.Attribute.PRIMITIVE, null)
                  ).asJava
                )
              ),
              FieldValue.of(
                FieldValue.Attribute.RECORD,
                FieldValueList.of(
                  List(
                    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "b"),
                    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "42.0")
                  ).asJava
                )
              )
            ).asJava
          )
        )
      ).asJava,
      (SchemaFor[OptionalShowCase].schema: BQSchema).getFields
    )

    val got = decoder.decode(EncodedRepr.Record(toDecode))
    val diff = Diff[OptionalShowCase].apply(expected, got.toTry.get)
    assert(diff.isIdentical, diff.show)
  }

  final case class AValueClass(a: Int) extends AnyVal
  final case class ValueClassShowcase(a: Option[AValueClass], b: AValueClass)

  test("Decoder should be derivable for a value class and work correctly") {
    val decoder = Decoder[ValueClassShowcase]

    val expected = ValueClassShowcase(
      a = None,
      b = AValueClass(42)
    )

    val toDecode = FieldValueList.of(
      List(
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, null),
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "42")
      ).asJava,
      (SchemaFor[ValueClassShowcase].schema: BQSchema).getFields
    )

    val got = decoder.decode(EncodedRepr.Record(toDecode))
    val diff = Diff[ValueClassShowcase].apply(expected, got.toTry.get)
    assert(diff.isIdentical, diff.show)
  }

  final case class RenamingShowcase(ageOfPerson: Double, dateOfBirth: LocalDate)

  test("Decoder should rename from snake_case to camelCase") {
    implicit val config: Configuration = Configuration().withSnakeCaseMemberNames
    val _ = config // TODO: fix unused
    val decoder = Decoder[RenamingShowcase]

    val expected = RenamingShowcase(42.2, LocalDate.of(1970, 1, 1))

    val toDecode = FieldValueList.of(
      List(
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "42.2"),
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1970-01-01")
      ).asJava,
      (SchemaFor[RenamingShowcase].schema: BQSchema).getFields
    )

    val got = decoder.decode(EncodedRepr.Record(toDecode))
    val diff = Diff[RenamingShowcase].apply(expected, got.toTry.get)
    assert(diff.isIdentical, diff.show)
  }
}
