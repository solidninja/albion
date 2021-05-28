import sbt._
import Keys._

import xerial.sbt.Sonatype.autoImport.sonatypeProfileName

object build {

  val manifestSetting = packageOptions += {
    val (title, v, vendor) = (name.value, version.value, organization.value)
    Package.ManifestAttributes(
      "Created-By" -> "Simple Build Tool",
      "Built-By" -> System.getProperty("user.name"),
      "Build-Jdk" -> System.getProperty("java.version"),
      "Specification-Title" -> title,
      "Specification-Version" -> v,
      "Specification-Vendor" -> vendor,
      "Implementation-Title" -> title,
      "Implementation-Version" -> v,
      "Implementation-Vendor-Id" -> vendor,
      "Implementation-Vendor" -> vendor
    )
  }

  val mavenCentralSettings = Seq(
    description := "Scala API for Google Cloud BigQuery",
    homepage := Some(url("https://gitlab.com/solidninja/albion")),
    startYear := Some(2019),
    licenses += "MIT" -> url("https://opensource.org/licenses/mit-license.php"),
    developers := List(
      Developer(
        id = "vladimir-lu",
        name = "Vladimir Lushnikov",
        email = "vladimir@solidninja.is",
        url = url("https://solidninja.is")
      )
    ),
    scmInfo := Some(
      ScmInfo(
        url("https://gitlab.com/solidninja/albion"),
        "scm:git:https://gitlab.com/solidninja/albion.git",
        Some(s"scm:git:git@gitlab.com:solidninja/albion.git")
      )
    )
  )

  val commonSettings = mavenCentralSettings ++ Seq(
    organization := "is.solidninja.albion",
    version := "0.1.0",
    scalaVersion := "2.13.3",
    crossScalaVersions := Seq("2.12.14", "2.13.3"),
    javacOptions ++= Seq("-target", "1.8", "-source", "1.8"),
    manifestSetting,
    crossVersion := CrossVersion.binary,
    // use minitest
    testFrameworks += new TestFramework("minitest.runner.Framework")
  )

  val publishSettings = Seq(
    publishTo := Some(
      if (isSnapshot.value)
        Opts.resolver.sonatypeSnapshots
      else
        Opts.resolver.sonatypeStaging
    ),
    publishMavenStyle := true,
    publishArtifact in Test := false,
    sonatypeProfileName := "is.solidninja"
  )
}
