package is
package solidninja
package albion

import java.time.{Instant, LocalDate, LocalDateTime}
import java.util.UUID

import cats.effect.{IO, Resource}
import cats.instances.either._
import cats.instances.list._
import cats.syntax.show._
import cats.syntax.traverse._
import com.danielasfregola.randomdatagenerator.magnolia.RandomDataGenerator
import com.softwaremill.diffx._
import com.typesafe.scalalogging.StrictLogging
import is.solidninja.albion.ArbitraryTime._
import minitest._

object BigQueryIntegrationTest extends SimpleTestSuite with StrictLogging with RandomDataGenerator {
  val IntegrationProject = ProjectId("snl-integration-test")
  val IntegrationDataset = DatasetId(IntegrationProject.project, "albion")

  def client: IO[BigQuery[IO]] = BigQuery(IntegrationProject, location = Some("US"))

  def tempTable[A: SchemaFor](client: BigQuery[IO]): Resource[IO, TableId] = {
    val id = s"ittest_${UUID.randomUUID().toString.replace("-", "_")}"
    val tableId = TableId(IntegrationDataset, id)
    val schema = SchemaFor[A].schema
    logger.debug(show"Using temporary table $tableId for test")
    Resource.make(client.createTable(tableId, schema))(client.deleteTable(_).map(_ => ()))
  }

  implicit val configuration: Configuration = Configuration().withSnakeCaseMemberNames

  // demonstration of case class decoding
  final case class CountryCode(code: String) extends AnyVal
  final case class Customer(id: Long, lat: Option[Double], lon: Option[Double])
  final case class Tag(tag: String) extends AnyVal
  final case class Meta(requestId: Option[String])
  final case class TagComment(tag: String, comment: String)
  final case class CustomerLocationEvent(
      id: UUID,
      eventType: String,
      eventDate: LocalDate,
      createdAt: Instant,
      updatedAt: Option[LocalDateTime],
      brand: String,
      country: CountryCode,
      tags: Option[Vector[Tag]],
      customer: Option[Customer],
      comments: Option[Seq[TagComment]],
      meta: Option[Meta] = None
  )

  test("Creating a temporary table, populating it, and reading back the results should work correctly") {
    val events = random[CustomerLocationEvent](20)
    val eventsWithIds = events.zipWithIndex.map(_.swap).map {
      case (idx, event) => s"event-$idx" -> event
    }

    val got = client.flatMap(client => tempTable[CustomerLocationEvent](client).use { tableId =>
      for {
        response <- client.insert[CustomerLocationEvent](tableId, eventsWithIds)
        _ = assert(!response.hasErrors, s"expecting response not to have any errors, errors were: $response")
        rows <- client.query[CustomerLocationEvent](SQLString(show"select * from `$tableId`"))
      } yield rows.iterator.toList.sequence
    }).unsafeRunSync()

    assert(got.isRight, s"expected Right(_), got $got")
    val gotList = got.getOrElse(sys.error("right.get"))

    events.foreach { event =>
      val foundOpt = gotList.find(_.id == event.id)
      assert(foundOpt.nonEmpty, s"expected to find event ${event.id}, but got None")

      // pretty print diffs
      val fixedExpected = event.copy(
        createdAt = clipToMillis(event.createdAt),
        meta = normalizeMeta(event.meta),
        comments = normalizeComments(event.comments),
        tags = normalizeTags(event.tags)
      )
      val fixedFound = foundOpt.get.copy(createdAt = clipToMillis(foundOpt.get.createdAt))

      val diff = Diff[CustomerLocationEvent].apply(fixedExpected, fixedFound)
      assert(diff.isIdentical, diff.show)
    }
  }

  // the case where the row cell is Some(Meta(None)) is not representable in BigQuery and will result in None (and
  //  comparison failure)
  private def normalizeMeta(meta: Option[Meta]): Option[Meta] = meta match {
    case Some(Meta(None)) => None
    case _ => meta
  }

  private def normalizeComments(comments: Option[Seq[TagComment]]): Option[Seq[TagComment]] = comments match {
    case Some(Nil) => None
    case _ => comments
  }

  private def normalizeTags(tags: Option[Vector[Tag]]): Option[Vector[Tag]] = tags match {
    case Some(Vector()) => None
    case _ => tags
  }

  // there is a rounding error in the double-encoded timestamp that means it's not exactly the same :|
  private def clipToMillis(i: Instant): Instant = Instant.ofEpochSecond(i.getEpochSecond, (i.getNano / 1000000L) * 1000000L)
}
