package is
package solidninja
package albion

import com.google.cloud.bigquery.{TableId => BQTableId, DatasetId => BQDatasetId, JobId => BQJobId}

import cats.Show

/** GCloud Project Id
  */
final case class ProjectId(project: String) extends AnyVal

object ProjectId {
  implicit val showProjectId: Show[ProjectId] = Show.show(_.project)
}

/** GCloud Dataset Id, a tuple of (project, dataset)
  */
final case class DatasetId(project: String, dataset: String)

object DatasetId {

  def apply(project: String, dataset: String): DatasetId = new DatasetId(project, dataset)

  implicit val showDatasetId: Show[DatasetId] = Show.show(id => s"${id.project}.${id.dataset}")

  implicit def toBQDatasetId(id: DatasetId): BQDatasetId = BQDatasetId.of(id.project, id.dataset)
  implicit def fromBQDatasetId(id: BQDatasetId): DatasetId = DatasetId(id.getProject, id.getDataset)
}

/** GCloud Table Id, a tuple of (project, dataset, table)
  */
final case class TableId(project: String, dataset: String, table: String)

object TableId {

  def apply(project: String, dataset: String, table: String) = new TableId(project, dataset, table)
  def apply(datasetId: DatasetId, table: String): TableId = new TableId(datasetId.project, datasetId.dataset, table)

  implicit val showTableId: Show[TableId] = Show.show(id => s"${id.project}.${id.dataset}.${id.table}")

  implicit def toBQTableId(id: TableId): BQTableId = BQTableId.of(id.project, id.dataset, id.table)
  implicit def fromBQTableId(id: BQTableId): TableId = TableId(id.getProject, id.getDataset, id.getTable)
}

/** GCloud Job Id
  */
final case class JobId(jobId: String) extends AnyVal

object JobId {
  implicit val showJobId: Show[JobId] = Show.show(_.jobId)

  implicit def toBQJobId(id: JobId): BQJobId = BQJobId.of(id.jobId)
  implicit def fromBQJobId(id: BQJobId): JobId = JobId(id.getJob)
}
