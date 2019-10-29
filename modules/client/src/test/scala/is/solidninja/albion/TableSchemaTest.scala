package is
package solidninja
package albion

import minitest.SimpleTestSuite
import com.google.cloud.bigquery.{StandardSQLTypeName, Field => BQField, Schema => BQSchema}

object TableSchemaTest extends SimpleTestSuite {
  test("A table schema must be convertible to the library representation") {
    val schema: TableSchema = TableSchema(
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

    val expected: BQSchema = BQSchema.of(
      BQField.newBuilder("commit", StandardSQLTypeName.STRING).setMode(BQField.Mode.REQUIRED).build(),
      BQField.newBuilder("tree", StandardSQLTypeName.STRING).setMode(BQField.Mode.REQUIRED).build(),
      BQField.newBuilder("parent", StandardSQLTypeName.STRING).setMode(BQField.Mode.REPEATED).build(),
      BQField
        .newBuilder(
          "author",
          StandardSQLTypeName.STRUCT,
          BQField.newBuilder("name", StandardSQLTypeName.STRING).setMode(BQField.Mode.NULLABLE).build(),
          BQField.newBuilder("email", StandardSQLTypeName.STRING).setMode(BQField.Mode.NULLABLE).build(),
          BQField.newBuilder("timestamp", StandardSQLTypeName.TIMESTAMP).setMode(BQField.Mode.NULLABLE).build()
        )
        .setMode(BQField.Mode.NULLABLE)
        .build(),
      BQField.newBuilder("message", StandardSQLTypeName.STRING).setMode(BQField.Mode.NULLABLE).build()
    )

    assertEquals(schema: BQSchema, expected)
    assertEquals(expected: TableSchema, schema)
  }

  test("Schema types should be mapped correctly") {
    assertEquals(SQLType.bool.`type`, StandardSQLTypeName.BOOL)
    assertEquals(TableField("a").bool.`type`.`type`, StandardSQLTypeName.BOOL)

    assertEquals(SQLType.bytes.`type`, StandardSQLTypeName.BYTES)
    assertEquals(TableField("a").bytes.`type`.`type`, StandardSQLTypeName.BYTES)

    assertEquals(SQLType.date.`type`, StandardSQLTypeName.DATE)
    assertEquals(TableField("a").date.`type`.`type`, StandardSQLTypeName.DATE)

    assertEquals(SQLType.datetime.`type`, StandardSQLTypeName.DATETIME)
    assertEquals(TableField("a").datetime.`type`.`type`, StandardSQLTypeName.DATETIME)

    assertEquals(SQLType.double.`type`, StandardSQLTypeName.FLOAT64)
    assertEquals(TableField("a").double.`type`.`type`, StandardSQLTypeName.FLOAT64)

    assertEquals(SQLType.geography.`type`, StandardSQLTypeName.GEOGRAPHY)
    assertEquals(TableField("a").geography.`type`.`type`, StandardSQLTypeName.GEOGRAPHY)

    assertEquals(SQLType.long.`type`, StandardSQLTypeName.INT64)
    assertEquals(TableField("a").long.`type`.`type`, StandardSQLTypeName.INT64)

    assertEquals(SQLType.numeric.`type`, StandardSQLTypeName.NUMERIC)
    assertEquals(TableField("a").numeric.`type`.`type`, StandardSQLTypeName.NUMERIC)

    assertEquals(SQLType.string.`type`, StandardSQLTypeName.STRING)
    assertEquals(TableField("a").string.`type`.`type`, StandardSQLTypeName.STRING)

    assertEquals(SQLType.time.`type`, StandardSQLTypeName.TIME)
    assertEquals(TableField("a").time.`type`.`type`, StandardSQLTypeName.TIME)

    assertEquals(SQLType.timestamp.`type`, StandardSQLTypeName.TIMESTAMP)
    assertEquals(TableField("a").timestamp.`type`.`type`, StandardSQLTypeName.TIMESTAMP)
  }
}
