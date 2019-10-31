package is
package solidninja
package albion

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

import cats.Show
import cats.effect.Sync
import cats.instances.string._
import cats.syntax.apply._
import cats.syntax.functor._
import cats.syntax.show._
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bigquery.BigQuery.{DatasetDeleteOption => BQDatasetDeleteOption}
import com.google.cloud.bigquery.{
  BigQuery => BQuery,
  BigQueryOptions => BQOptions,
  DatasetInfo => BQDatasetInfo,
  InsertAllRequest => BQInsertAllRequest,
  InsertAllResponse => BQInsertAllResponse,
  JobInfo => BQJobInfo,
  QueryJobConfiguration => BQueryJobConfiguration,
  StandardTableDefinition => BQStandardTableDefinition,
  TableInfo => BQTableInfo,
  TimePartitioning => BQTimePartitioning
}
import com.typesafe.scalalogging.StrictLogging

/**
  * Unsafe wrapper for a string containing SQL.
  */
final case class SQLString(sql: String) extends AnyVal

object SQLString {
  implicit val showSQL: Show[SQLString] = Show.show(_.sql)
}

/**
  * Table time partitioning parameters
  *
  * @see https://cloud.google.com/bigquery/docs/creating-column-partitions
  */
final case class TablePartitioning(
    `type`: BQTimePartitioning.Type,
    column: Option[String] = None,
    expiration: Option[FiniteDuration] = None
)

/**
  * Client for the BigQuery API (a safe wrapper for the Java API)
  */
trait BigQuery[F[_]] {

  /**
    * Create a new dataset by name
    */
  def createDataset(id: DatasetId): F[DatasetId]

  /**
    * Create a new dataset with more parameters
    */
  def createDataset(info: BQDatasetInfo): F[DatasetId]

  /**
    * Create a new job with more parameters
    */
  def createJob(info: BQJobInfo): F[BQJobInfo]

  /**
    * Create a new standard table inside a dataset, with given schema and optional time partitioning
    */
  def createTable(id: TableId, schema: TableSchema, partitioning: Option[TablePartitioning] = None): F[TableId]

  /**
    * Create a table with a more expanded range of parameters (e.g. an external table)
    */
  def createTable(info: BQTableInfo): F[TableId]

  /**
    * Delete a dataset. By default, contents are not deleted if the dataset is non-empty
    */
  def deleteDataset(id: DatasetId, deleteContents: Boolean = false): F[Boolean]

  /**
    * Delete a table and its contents
    */
  def deleteTable(id: TableId): F[Boolean]

  /**
    * Get a dataset's metadata
    */
  def getDataset(id: DatasetId): F[Option[BQDatasetInfo]]

  /**
    * Get a job's metadata
    */
  def getJob(id: JobId): F[Option[BQJobInfo]]

  /**
    * Get a table's metadata
    */
  def getTable(id: TableId): F[Option[BQTableInfo]]

  /**
    * Insert rows into a table. Each row has a string row id, that can be used to correlate the row in the response
    *
    * @note This API currently leaves error checking to the user
    */
  def insert[A: Encoder](
      id: TableId,
      rows: Seq[(String, A)],
      ignoreUnknownValues: Boolean = false
  ): F[BQInsertAllResponse]

  /**
    * Run a SQL query based on a string, returning an iterable of decoded results
    */
  def query[A: Decoder](sql: SQLString): F[BigQuery.IterableResult[Either[DecodingError, A]]]

  /**
    * Run a query based on an expanded query job configuration, returning an iterable of decoded results
    */
  def query[A: Decoder](config: BQueryJobConfiguration): F[BigQuery.IterableResult[Either[DecodingError, A]]]

}

object BigQuery extends StrictLogging {
  type IterableResult[A] = is.solidninja.albion.IterableResult[A]
  val IterableResult = is.solidninja.albion.IterableResult

  def apply[F[_]: Sync](projectId: ProjectId, location: Option[String]): F[BigQuery[F]] =
    rawBigQuery(projectId, location).map(fromRaw[F])

  private def rawBigQuery[F[_]: Sync](projectId: ProjectId, location: Option[String]): F[BQuery] =
    Sync[F].delay {
      val transportOptions = BQOptions.getDefaultHttpTransportOptions
      val credentials = GoogleCredentials.getApplicationDefault

      val opts = location
        .fold(BQOptions.newBuilder())(l => BQOptions.newBuilder().setLocation(l))
        .setCredentials(credentials)
        .setProjectId(projectId.show)
        .setTransportOptions(transportOptions)
        .build()

      opts.getService
    }

  /**
    * Create the BigQuery client directly from the Java API client
    */
  def fromRaw[F[_]: Sync](client: BQuery): BigQuery[F] = new BigQuery[F] {

    override def createDataset(id: DatasetId): F[DatasetId] = createDataset(BQDatasetInfo.of(id))

    override def createDataset(info: BQDatasetInfo): F[DatasetId] =
      log(show"Creating dataset: ${info.toString}") *>
        Sync[F]
          .delay(client.create(info))
          .map(t => t.getDatasetId)

    override def createJob(info: BQJobInfo): F[BQJobInfo] =
      log(show"Creating job: ${info.toString}") *>
        Sync[F]
          .delay(client.create(info): BQJobInfo) // note this exposes the Job class, which is mutable

    override def createTable(id: TableId, schema: TableSchema, partitioning: Option[TablePartitioning]): F[TableId] = {
      val timePartitioning = partitioning.map { p =>
        val b = BQTimePartitioning.newBuilder(p.`type`)
        p.column.foreach(b.setField)
        p.expiration.foreach(d => b.setExpirationMs(d.toMillis))
        b.build()
      }.orNull

      val definition = BQStandardTableDefinition
        .newBuilder()
        .setSchema(schema)
        .setTimePartitioning(timePartitioning)
        .build()

      createTable(BQTableInfo.of(id, definition))
    }

    override def createTable(info: BQTableInfo): F[TableId] =
      log(show"Creating table: ${info.toString}") *>
        Sync[F]
          .delay(client.create(info))
          .map(t => t.getTableId)

    override def deleteDataset(id: DatasetId, deleteContents: Boolean): F[Boolean] =
      log(show"Deleting dataset: $id") *>
        Sync[F].delay {
          val opts = if (deleteContents) List(BQDatasetDeleteOption.deleteContents()) else Nil
          client.delete(id, opts: _*)
        }

    override def deleteTable(id: TableId): F[Boolean] =
      log(show"Deleting table: $id") *>
        Sync[F].delay(client.delete(id))

    override def getDataset(id: DatasetId): F[Option[BQDatasetInfo]] = Sync[F].delay(Option(client.getDataset(id)))

    override def getJob(id: JobId): F[Option[BQJobInfo]] = Sync[F].delay(Option(client.getJob(id)))

    override def getTable(id: TableId): F[Option[BQTableInfo]] = Sync[F].delay(Option(client.getTable(id)))

    override def insert[A](id: TableId, rows: Seq[(String, A)], ignoreUnknownValues: Boolean)(
        implicit encoder: Encoder[A]
    ): F[BQInsertAllResponse] = Sync[F].delay {
      val req = rows
        .foldLeft(BQInsertAllRequest.newBuilder(id).setIgnoreUnknownValues(ignoreUnknownValues)) {
          case (b, (id, contents)) => b.addRow(id, encoder.encode(contents))
        }
        .build()

      client.insertAll(req)
    }

    override def query[A: Decoder](sql: SQLString): F[BigQuery.IterableResult[Either[DecodingError, A]]] =
      query(BQueryJobConfiguration.of(sql.sql))

    override def query[A](
        config: BQueryJobConfiguration
    )(implicit decoder: Decoder[A]): F[IterableResult[Either[DecodingError, A]]] =
      log(show"Executing query: ${config.getQuery}") *>
        Sync[F]
          .delay(client.query(config))
          .map(
            res =>
              IterableResult[Either[DecodingError, A]](
                res.getTotalRows,
                res
                  .iterateAll()
                  .asScala
                  .iterator
                  .map(row => decoder.decode(EncodedRepr.Record(row)))
              )
          )

    private def log[T: Show](msg: => T): F[Unit] = Sync[F].delay(logger.debug(msg.show))
  }
}
