import sbt._

object Dependencies {

  object Versions {
    val cats = "2.1.0"
    val `cats-effect` = "2.1.2"
    val diffx = "0.3.15"
    val `google-cloud` = "1.102.0"
    val magnolia = "0.12.5"
    val minitest = "2.7.0"
    val `random-data-generator` = "2.8"
    val `scala-collections-compat` = "2.1.3"
    val `scala-logging` = "3.9.2"
  }

  val bigquery = Seq(
    "com.google.cloud" % "google-cloud-bigquery" % Versions.`google-cloud`
  )

  val `cats-effect` = Seq(
    "org.typelevel" %% "cats-core" % Versions.cats,
    "org.typelevel" %% "cats-effect" % Versions.`cats-effect`
  )

  val `collections-compat` = Seq(
    "org.scala-lang.modules" %% "scala-collection-compat" % Versions.`scala-collections-compat`
  )

  val magnolia = Seq(
    "com.propensive" %% "magnolia" % Versions.magnolia
  )

  val scalatest = Seq(
    "com.danielasfregola" %% "random-data-generator-magnolia" % Versions.`random-data-generator` % "it,test",
    "com.softwaremill.diffx" %% "diffx-core" % Versions.diffx % "it,test",
    "io.monix" %% "minitest" % Versions.minitest % "it,test"
  )

  val runtimeLogging = Seq(
    "com.typesafe.scala-logging" %% "scala-logging" % Versions.`scala-logging`,
    "ch.qos.logback" % "logback-classic" % "1.2.3" % "runtime"
  )
}
