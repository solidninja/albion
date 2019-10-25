import sbt._

object Dependencies {

  object Versions {
    val `google-cloud` = "1.98.0"
    val magnolia = "0.12.0"
    val minitest = "2.7.0"
  }

  val bigquery = Seq(
    "com.google.cloud" % "google-cloud-bigquery" % Versions.`google-cloud`
  )

  val magnolia = Seq(
    "com.propensive" %% "magnolia" % Versions.magnolia
  )

  val scalatest = Seq(
    "io.monix" %% "minitest" % Versions.minitest % Test
  )

  val runtimeLogging = Seq(
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
    "ch.qos.logback" % "logback-classic" % "1.2.3" % "runtime"
  )
}
