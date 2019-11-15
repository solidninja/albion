package is
package solidninja
package albion
package example

import cats.effect.{ExitCode, IO, IOApp}
import is.solidninja.albion._

final case class Author(name: String, books: List[String])

object AuthorsExample extends IOApp {
  override def run(args: List[String]): IO[ExitCode] =
    for {
      bq <- BigQuery[IO](ProjectId("gcp-project-id"), location = None)
      result <- bq.query[Author](SQLString("select name, books from `my-authors-dataset`"))
      _ = result.iterator.next()
    } yield ExitCode.Success
}
