package is
package solidninja
package albion
package example

import cats.{Functor, MonadError}
import cats.instances.either._
import cats.instances.list._
import cats.syntax.functor._
import cats.syntax.monadError._
import cats.syntax.traverse._
import cats.effect.{ExitCode, IO, IOApp}

final case class ObjectName(sha1: String) extends AnyVal

final case class Date(seconds: Option[Long], nanos: Option[Long])

final case class Person(
    name: Option[String],
    email: Option[String],
    timeSec: Option[Long],
    tzOffset: Option[Long],
    date: Option[Date]
)

final case class Trailer(key: Option[String], value: Option[String], email: Option[String])

final case class Difference(
    oldMode: Option[Long],
    newMode: Option[Long],
    oldPath: Option[String],
    newPath: Option[String],
    oldSha1: Option[String],
    newSha1: Option[String],
    oldRepo: Option[String],
    newRepo: Option[String]
)

final case class GithubCommit(
    commit: Option[ObjectName],
    tree: Option[ObjectName],
    parent: Seq[ObjectName],
    author: Option[Person],
    committer: Option[Person],
    subject: Option[String],
    message: Option[String],
    trailer: Seq[Trailer],
    difference: Option[Difference],
    differenceTruncated: Option[Boolean],
    repoName: Seq[String],
    encoding: Option[String]
)

class ListGithubCommitsExampleApp[F[_]: Functor]()(implicit bq: BigQuery[F], me: MonadError[F, Throwable]) {

  implicit val config = Configuration().withSnakeCaseMemberNames

  def run: F[Unit] = {
    bq.query[GithubCommit](SQLString("select * FROM `bigquery-public-data.github_repos.commits` LIMIT 1000"))
      .map(_.iterator.toList.sequence)
      .rethrow
      .map(_.foreach(printCommit))
  }

  private def printCommit(commit: GithubCommit): Unit = {
    // todo: make test output more imaginative
    println(commit)
  }
}

/** Example app to query `bigquery-public-data.github_repos.commits` and print out results
  *
  * @note
  *   You need to be authenticated with gcloud and have the BigQuery API enabled to run this successfully.
  */
object ListGithubCommitsExampleApp extends IOApp {
  override def run(args: List[String]): IO[ExitCode] =
    BigQuery[IO](ProjectId(sys.env.getOrElse("GOOGLE_CLOUD_PROJECT", "snl-integration-test")), Some("US"))
      .flatMap { implicit bq =>
        new ListGithubCommitsExampleApp[IO]().run
      }
      .map(_ => ExitCode.Success)
}
