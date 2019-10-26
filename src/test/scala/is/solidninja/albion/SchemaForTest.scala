package is
package solidninja
package albion

import java.time.{Instant, LocalDate, LocalDateTime}
import java.util.UUID

import minitest._

object SchemaForTest extends SimpleTestSuite {

  final case class CommitAuthor(name: Option[String], email: Option[String], timestamp: Option[Instant])

  final case class CommitRecord(
      commit: String,
      tree: String,
      parent: Seq[String],
      author: Option[CommitAuthor],
      message: Option[String]
  )

  test("CommitRecord should have derivable schema") {
    val expected: TableSchema = TableSchema(
      TableField("commit").string,
      TableField("tree").string,
      TableField("parent").repeated.string,
      TableField("author").nullable.struct(
        TableField("name").nullable.string,
        TableField("email").nullable.string,
        TableField("timestamp").nullable.timestamp
      ),
      TableField("message").nullable.string
    )

    val got = SchemaFor[CommitRecord].schema
    assertEquals(got, expected)
  }

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

  test("primitive types should have expected schema derived") {
    val expected: TableSchema = TableSchema(
      TableField("a").bytes,
      TableField("b").bool,
      TableField("c").double,
      TableField("d").long,
      TableField("e").timestamp,
      TableField("f").date,
      TableField("g").datetime,
      TableField("h").long,
      TableField("i").string,
      TableField("j").string,
      TableField("k").numeric,
      TableField("l").double
    )

    val got = SchemaFor[PrimitiveShowcase].schema
    assertEquals(got, expected)
  }

  final case class PrimitiveOptionalShowcase(
      a: Option[Array[Byte]],
      b: Option[Boolean],
      c: Option[Double],
      d: Option[Int],
      e: Option[Instant],
      f: Option[LocalDate],
      g: Option[LocalDateTime],
      h: Option[Long],
      i: Option[String],
      j: Option[UUID],
      k: Option[BigDecimal],
      l: Option[Float]
  )

  test("primitive optional types should have expected schema derived") {
    val expected: TableSchema = TableSchema(
      TableField("a").nullable.bytes,
      TableField("b").nullable.bool,
      TableField("c").nullable.double,
      TableField("d").nullable.long,
      TableField("e").nullable.timestamp,
      TableField("f").nullable.date,
      TableField("g").nullable.datetime,
      TableField("h").nullable.long,
      TableField("i").nullable.string,
      TableField("j").nullable.string,
      TableField("k").nullable.numeric,
      TableField("l").nullable.double
    )

    val got = SchemaFor[PrimitiveOptionalShowcase].schema
    assertEquals(got, expected)
  }

  final case class PrimitiveRepeatedShowcase(
      a: Seq[Array[Byte]],
      b: Seq[Boolean],
      c: Seq[Double],
      d: Seq[Int],
      e: Seq[Instant],
      f: Seq[LocalDate],
      g: Seq[LocalDateTime],
      h: Seq[Long],
      i: Seq[String],
      j: Seq[UUID],
      k: Seq[BigDecimal],
      l: Seq[Float]
  )

  test("primitive repeated types should have expected schema derived") {
    val expected: TableSchema = TableSchema(
      TableField("a").repeated.bytes,
      TableField("b").repeated.bool,
      TableField("c").repeated.double,
      TableField("d").repeated.long,
      TableField("e").repeated.timestamp,
      TableField("f").repeated.date,
      TableField("g").repeated.datetime,
      TableField("h").repeated.long,
      TableField("i").repeated.string,
      TableField("j").repeated.string,
      TableField("k").repeated.numeric,
      TableField("l").repeated.double
    )

    val got = SchemaFor[PrimitiveRepeatedShowcase].schema
    assertEquals(got, expected)
  }

  final case class OptionalRepeatedShowcase(a: Seq[Option[String]], b: Option[Seq[String]])

  test("optional repeated fields should have expected schema derived") {
    val expected: TableSchema = TableSchema(
      TableField("a").repeated.string,
      TableField("b").repeated.string
    )

    val got = SchemaFor[OptionalRepeatedShowcase].schema
    assertEquals(got, expected)
  }

  final case class SampleStruct(a: String, b: Seq[Int], c: Option[Double])

  final case class OptionalRepeatedStructShowcase(
      a: Option[SampleStruct],
      b: Seq[SampleStruct],
      c: Option[Seq[SampleStruct]],
      d: Seq[Option[SampleStruct]]
  )

  test("optional repeated struct fields should have expected schema derived") {
    val structSchema = List(
      TableField("a").string,
      TableField("b").repeated.long,
      TableField("c").nullable.double
    )

    val expected: TableSchema = TableSchema(
      TableField("a").nullable.struct(structSchema),
      TableField("b").repeated.struct(structSchema),
      TableField("c").repeated.struct(structSchema),
      TableField("d").repeated.struct(structSchema)
    )

    val got = SchemaFor[OptionalRepeatedStructShowcase].schema
    assertEquals(got, expected)
  }

  final case class CustomId(a: String, b: Unit)

  object CustomId {
    implicit val displayAsString: DisplayAsString[CustomId] = DisplayAsString(
      s => Right(CustomId(s, ())),
      _.a
    )
  }

  final case class CustomDisplayAsStringShowcase(a: CustomId, b: Option[CustomId], c: Seq[CustomId], d: Int)

  test("custom types that can be displayed as string should have expected schema derived") {
    val expected: TableSchema = TableSchema(
      TableField("a").required.string,
      TableField("b").nullable.string,
      TableField("c").repeated.string,
      TableField("d").long
    )

    val got = SchemaFor[CustomDisplayAsStringShowcase].schema
    assertEquals(got, expected)
  }

  final case class DifferentSeqTypesShowcase(a: Seq[String], b: List[String], c: Vector[String], d: Array[String])

  test("different seq types should have expected schema derived") {
    val expected: TableSchema = TableSchema(
      TableField("a").repeated.string,
      TableField("b").repeated.string,
      TableField("c").repeated.string,
      TableField("d").repeated.string
    )

    val got = SchemaFor[DifferentSeqTypesShowcase].schema
    assertEquals(got, expected)
  }

  final case class SnakeCaseShowcase(personName: String, ageOfElephant: Option[Double])

  test("converting camel case field names to snake_case should work") {
    val expected: TableSchema = TableSchema(
      TableField("person_name").string,
      TableField("age_of_elephant").nullable.double
    )

    implicit val config: Configuration = Configuration().withSnakeCaseMemberNames
    val _ = config
    val got = SchemaFor[SnakeCaseShowcase].schema
    assertEquals(got, expected)
  }
}
