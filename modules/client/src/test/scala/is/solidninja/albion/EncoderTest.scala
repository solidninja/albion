package is
package solidninja
package albion

import java.time.{Instant, LocalDate, LocalDateTime}
import java.util.{Base64, UUID}

import scala.jdk.CollectionConverters._

import minitest._

object EncoderTest extends SimpleTestSuite {

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
      l: Float
  )

  test("Encoder for primitives should be derived and produce correct results") {
    val encoder = Encoder[PrimitiveShowcase]

    val bytesHex = "CfkRAp1041vYQVbFY1aIwA=="
    val uuid = "56e1b780-7795-4b10-9cad-e4b337e79be4"

    val toEncode = PrimitiveShowcase(
      a = Base64.getDecoder.decode(bytesHex),
      b = true,
      c = 1 / 10.0,
      d = 42,
      e = Instant.ofEpochMilli(1571788800052L),
      f = LocalDate.of(9999, 12, 31),
      g = LocalDateTime.of(2012, 8, 12, 3, 45, 30, 123456789),
      h = Long.MaxValue,
      i = "hello world",
      j = UUID.fromString(uuid),
      k = BigDecimal("1238126387123.1234"),
      l = 42.1f
    )

    val expected = Map[String, AnyRef](
      "a" -> bytesHex,
      "b" -> (true: java.lang.Boolean),
      "c" -> (0.1: java.lang.Double),
      "d" -> (42L: java.lang.Long),
      "e" -> "2019-10-23T00:00:00.052000Z",
      "f" -> "9999-12-31",
      "g" -> "2012-08-12T03:45:30.123456",
      "h" -> (9223372036854775807L: java.lang.Long),
      "i" -> "hello world",
      "j" -> uuid,
      "k" -> BigDecimal("1238126387123.1234").bigDecimal,
      "l" -> (42.099998474121094: java.lang.Double)
    ).asJava

    val got = encoder.encode(toEncode)
    assertEquals(got, expected)
  }

  final case class OptionalAndSequenceShowcase(
      a: Option[UUID],
      b: Option[Seq[Int]],
      c: Seq[Option[String]]
  )

  test("Encoder for Option and Seq should interleave and encode values correctly when values are null") {
    val encoder = Encoder[OptionalAndSequenceShowcase]

    val toEncode = OptionalAndSequenceShowcase(
      a = None,
      b = None,
      c = List(None, None, None)
    )

    val expected = Map[String, AnyRef](
      "a" -> null,
      "b" -> null,
      "c" -> List(null, null, null).asJava
    ).asJava

    val got = encoder.encode(toEncode)
    assertEquals(got, expected)
  }

  test("Encoder for Option and Seq should interleave and encode values correctly when values are not null") {
    val encoder = Encoder[OptionalAndSequenceShowcase]

    val uuid = "889eb47f-ac63-4f43-848a-0b0a7ade2733"
    val toEncode = OptionalAndSequenceShowcase(
      a = Some(UUID.fromString(uuid)),
      b = Some(List(2, 4, 6, 8, 10)),
      c = List(None, Some("foo"), Some("bar"))
    )

    val expected = Map[String, AnyRef](
      "a" -> uuid,
      "b" -> List(2L, 4L, 6L, 8L, 10L).asJava,
      "c" -> List(null, "foo", "bar").asJava // TODO: check whether we should filter nulls though!
    ).asJava

    val got = encoder.encode(toEncode)
    assertEquals(got, expected)
  }

  final case class InnerShowcase(name: String, age: Double)
  final case class OuterShowcase(inner: InnerShowcase, b: String)

  test("Encoder for nested case class should work correctly") {
    val encoder = Encoder[OuterShowcase]

    val toEncode = OuterShowcase(InnerShowcase("bob", 42.0), "id-21")

    val expected = Map[String, AnyRef](
      "inner" -> Map[String, AnyRef](
        "name" -> "bob",
        "age" -> (42.0: java.lang.Double)
      ).asJava,
      "b" -> "id-21"
    ).asJava

    val got = encoder.encode(toEncode)
    assertEquals(got, expected)
  }

  final case class AnyValShowcase(v: String) extends AnyVal
  final case class AnyValContainer(name: AnyValShowcase, b: Long)

  test("Encoder for AnyVal should not produce an intermediate map") {
    val encoder = Encoder[AnyValContainer]
    val toEncode = AnyValContainer(AnyValShowcase("test"), 42L)

    val expected = Map[String, AnyRef](
      "name" -> "test",
      "b" -> (42L: java.lang.Long)
    ).asJava

    val got = encoder.encode(toEncode)
    assertEquals(got, expected)
  }

  final case class SequencesShowcase(a: Seq[String], b: List[String], c: Vector[String], d: Array[String])

  test("Encoder for sequences should produce a correct result") {
    val encoder = Encoder[SequencesShowcase]
    val toEncode = SequencesShowcase(List("a", "b", "c"), List("d", "e", "f"), Vector("g", "h", "i"), Array("j", "k"))

    val expected = Map[String, AnyRef](
      "a" -> List("a", "b", "c").asJava,
      "b" -> List("d", "e", "f").asJava,
      "c" -> List("g", "h", "i").asJava,
      "d" -> List("j", "k").asJava
    ).asJava

    val got = encoder.encode(toEncode)
    assertEquals(got, expected)
  }

  final case class RenamingShowcase(ageOfPerson: Double, dateOfBirth: LocalDate)

  test("Encoder should rename camelCase to snake_case") {
    implicit val config: Configuration = Configuration().withSnakeCaseMemberNames
    val _ = config // TODO: fix unused
    val encoder = Encoder[RenamingShowcase]

    val toEncode = RenamingShowcase(42.2, LocalDate.of(1970, 1, 1))

    val expected = Map[String, AnyRef](
      "age_of_person" -> (42.2: java.lang.Double),
      "date_of_birth" -> "1970-01-01"
    ).asJava

    val got = encoder.encode(toEncode)
    assertEquals(got, expected)
  }
}
