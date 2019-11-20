[![pipeline status](https://gitlab.com/solidninja/albion/badges/master/pipeline.svg)](https://gitlab.com/solidninja/albion/commits/master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/is.solidninja.albion/albion-client/badge.svg?style=plastic)](https://maven-badges.herokuapp.com/maven-central/is.solidninja.albion/albion-client)

# Albion, a BigQuery client for Scala

This library provides a typesafe interface over the [BigQuery Client Library][gcloud-bigquery-quickstart]. It uses 
Magnolia macros under the hood to derive the dataset schema and encoders/decoders for reading/writing data to a dataset.

## Getting started

To use, add the following to `build.sbt`:

`libraryDependencies += "is.solidninja.albion" %% "albion-client" % "0.1.0"`

For example, if you had a dataset of authors with the list of their books, you could write the following to query the 
dataset:

```scala
import cats.effect.IO
import is.solidninja.albion._

final case class Author(name: String, books: List[String])

BigQuery[IO](ProjectId("gcp-project-id"), location = None)
  .flatMap(_.query[Author](SQLString("select name, books from `my-authors-dataset`")))
  .map(_.iterator.next())
```

There are more examples in the [examples](modules/examples) project and in the tests. (More advanced examples like 
creating a column partitioned dataset with expiration are yet to be written).

To run the examples, you need to be authenticated with a Google Cloud account, for example via 
[`gcloud auth login`](gcloud-auth-login).

## License

This project is licensed as [MIT][mit-license]. 

[mit-license]: https://opensource.org/licenses/MIT
[gcloud-bigquery-quickstart]: https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries#complete_source_code
[gcloud-auth-login]: https://cloud.google.com/sdk/gcloud/reference/auth/login
