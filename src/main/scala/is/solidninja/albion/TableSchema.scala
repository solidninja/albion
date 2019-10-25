package is
package solidninja
package albion

import com.google.cloud.bigquery.{StandardSQLTypeName, Field => BQField, FieldList => BQFieldList, Schema => BQSchema}
import scala.jdk.CollectionConverters._

final case class SQLType(`type`: StandardSQLTypeName) extends AnyVal

object SQLType {
  val bool: SQLType = SQLType(StandardSQLTypeName.BOOL)
  val bytes: SQLType = SQLType(StandardSQLTypeName.BYTES)
  val date: SQLType = SQLType(StandardSQLTypeName.DATE)
  val datetime: SQLType = SQLType(StandardSQLTypeName.DATETIME)
  val double: SQLType = SQLType(StandardSQLTypeName.FLOAT64)
  val geography: SQLType = SQLType(StandardSQLTypeName.GEOGRAPHY)
  val long: SQLType = SQLType(StandardSQLTypeName.INT64)
  val numeric: SQLType = SQLType(StandardSQLTypeName.NUMERIC)
  val string: SQLType = SQLType(StandardSQLTypeName.STRING)
  val time: SQLType = SQLType(StandardSQLTypeName.TIME)
  val timestamp: SQLType = SQLType(StandardSQLTypeName.TIMESTAMP)
}

sealed trait TableField {
  def name: String
  def `type`: SQLType
  def mode: BQField.Mode
  def description: Option[String]
}

private[albion] sealed trait StructField extends TableField {
  override def `type`: SQLType = SQLType(StandardSQLTypeName.STRUCT)
  def fields: List[TableField]
}

private[albion] trait Required { self: TableField =>
  override def mode: BQField.Mode = BQField.Mode.REQUIRED
}

private[albion] trait Nullable { self: TableField =>
  override def mode: BQField.Mode = BQField.Mode.NULLABLE
}

private[albion] trait Repeated { self: TableField =>
  override def mode: BQField.Mode = BQField.Mode.REPEATED
}

object TableField {
  final case class PrimitiveRequired private[albion] (name: String, `type`: SQLType, description: Option[String])
      extends TableField
      with Required
  final case class PrimitiveNullable private[albion] (name: String, `type`: SQLType, description: Option[String])
      extends TableField
      with Nullable
  final case class PrimitiveRepeated private[albion] (name: String, `type`: SQLType, description: Option[String])
      extends TableField
      with Repeated

  final case class StructRequired private[albion] (name: String, fields: List[TableField], description: Option[String])
      extends StructField
      with Required
  final case class StructNullable private[albion] (name: String, fields: List[TableField], description: Option[String])
      extends StructField
      with Nullable
  final case class StructRepeated private[albion] (name: String, fields: List[TableField], description: Option[String])
      extends StructField
      with Repeated

  def apply(name: String): TableFieldBuilder = new TableFieldBuilder(name)
  def apply(name: String, mode: BQField.Mode): TableFieldBuilder = new TableFieldBuilder(name, mode)

  final class TableFieldBuilder private[TableField] (
      name: String,
      mode: BQField.Mode = BQField.Mode.REQUIRED,
      descriptionOpt: Option[String] = None
  ) {

    // Change mode of field
    def nullable: TableFieldBuilder = new TableFieldBuilder(name, BQField.Mode.NULLABLE)
    def repeated: TableFieldBuilder = new TableFieldBuilder(name, BQField.Mode.REPEATED)
    def required: TableFieldBuilder = new TableFieldBuilder(name, BQField.Mode.REQUIRED)

    // Change description
    def description(description: String) = new TableFieldBuilder(name, mode, Some(description))
    def description(descriptionOpt: Option[String]) = new TableFieldBuilder(name, mode, descriptionOpt)

    // Create primitive field
    def bool: TableField = primitive(SQLType.bool)
    def bytes: TableField = primitive(SQLType.bytes)
    def date: TableField = primitive(SQLType.date)
    def datetime: TableField = primitive(SQLType.datetime)
    def double: TableField = primitive(SQLType.double)
    def geography: TableField = primitive(SQLType.geography)
    def long: TableField = primitive(SQLType.long)
    def numeric: TableField = primitive(SQLType.numeric)
    def string: TableField = primitive(SQLType.string)
    def time: TableField = primitive(SQLType.time)
    def timestamp: TableField = primitive(SQLType.timestamp)

    def primitive(`type`: SQLType): TableField = `type` match {
      case SQLType(StandardSQLTypeName.STRUCT) | SQLType(StandardSQLTypeName.ARRAY) =>
        throw new IllegalArgumentException("A STRUCT or ARRAY field is not primitive")
      case _ =>
        mode match {
          case BQField.Mode.NULLABLE => PrimitiveNullable(name, `type`, descriptionOpt)
          case BQField.Mode.REPEATED => PrimitiveRepeated(name, `type`, descriptionOpt)
          case BQField.Mode.REQUIRED => PrimitiveRequired(name, `type`, descriptionOpt)
        }
    }

    // Create struct field
    def struct(fields: List[TableField]): TableField = mode match {
      case BQField.Mode.NULLABLE => StructNullable(name, fields, descriptionOpt)
      case BQField.Mode.REPEATED => StructRepeated(name, fields, descriptionOpt)
      case BQField.Mode.REQUIRED => StructRequired(name, fields, descriptionOpt)
    }

    def struct(f1: TableField, fxs: TableField*): TableField = struct(f1 :: fxs.toList)
  }

  private def toBQField(field: TableField): BQField = {
    val builder = field match {
      case sf: StructField =>
        BQField.newBuilder(sf.name, sf.`type`.`type`, BQFieldList.of(sf.fields.map(toBQField): _*))
      case _ => BQField.newBuilder(field.name, field.`type`.`type`)
    }
    field.description
      .fold(builder)(builder.setDescription)
      .setMode(field.mode)
      .build()
  }

  private def fromBQField(field: BQField): TableField = (field.getMode, field.getType.getStandardType) match {
    case (_, StandardSQLTypeName.ARRAY) => throw new NotImplementedError("FIXME array types not yet handled")
    case (BQField.Mode.NULLABLE, StandardSQLTypeName.STRUCT) =>
      StructNullable(
        field.getName,
        field.getSubFields.iterator().asScala.map(fromBQField).toList,
        Option(field.getDescription)
      )
    case (BQField.Mode.REQUIRED, StandardSQLTypeName.STRUCT) =>
      StructRequired(
        field.getName,
        field.getSubFields.iterator().asScala.map(fromBQField).toList,
        Option(field.getDescription)
      )
    case (BQField.Mode.REPEATED, StandardSQLTypeName.STRUCT) =>
      StructRepeated(
        field.getName,
        field.getSubFields.iterator().asScala.map(fromBQField).toList,
        Option(field.getDescription)
      )
    case (BQField.Mode.NULLABLE, primitiveType) =>
      PrimitiveNullable(field.getName, SQLType(primitiveType), Option(field.getDescription))
    case (BQField.Mode.REQUIRED, primitiveType) =>
      PrimitiveRequired(field.getName, SQLType(primitiveType), Option(field.getDescription))
    case (BQField.Mode.REPEATED, primitiveType) =>
      PrimitiveRepeated(field.getName, SQLType(primitiveType), Option(field.getDescription))
  }

  implicit def convertToBQField(field: TableField): BQField = toBQField(field)
  implicit def convertFromBQField(field: BQField): TableField = fromBQField(field)
}

/**
  * Scala equivalent to [[com.google.cloud.bigquery.Schema]]
  */
final case class TableSchema(fields: List[TableField])

object TableSchema {
  def apply(f1: TableField, fxs: TableField*): TableSchema = new TableSchema(f1 :: fxs.toList)

  implicit def convertToBQSchema(schema: TableSchema): BQSchema =
    BQSchema.of(schema.fields.map(TableField.convertToBQField): _*)
  implicit def convertFromBQSchema(schema: BQSchema): TableSchema =
    TableSchema(schema.getFields.iterator().asScala.map(TableField.convertFromBQField).toList)
}
