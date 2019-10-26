import sbt._

object Dependencies {

  object Versions {
    val cats = "2.0.0"
    val `cats-effect` = "2.0.0"
    val fs2 = "2.0.1"
    val `google-cloud` = "1.98.0"
    val magnolia = "0.12.0"
    val minitest = "2.7.0"
    val `scala-logging` = "3.9.2"
  }

  val bigquery = Seq(
    "com.google.cloud" % "google-cloud-bigquery" % Versions.`google-cloud`
  )

  val `cats-effect` = Seq(
    "org.typelevel" %% "cats-core" % Versions.cats,
    "org.typelevel" %% "cats-effect" % Versions.`cats-effect`
  )

  val fs2 = Seq(
    "co.fs2" %% "fs2-core" % Versions.fs2
  )

  val magnolia = Seq(
    "com.propensive" %% "magnolia" % Versions.magnolia
  )

  val scalatest = Seq(
    "io.monix" %% "minitest" % Versions.minitest % Test
  )

  val runtimeLogging = Seq(
    "com.typesafe.scala-logging" %% "scala-logging" % Versions.`scala-logging`,
    "ch.qos.logback" % "logback-classic" % "1.2.3" % "runtime"
  )
}
