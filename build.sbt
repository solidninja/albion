import Dependencies._
import build._

Global / onChangedBuildSource := ReloadOnSourceChanges

// Enable pinentry
Global / useGpgPinentry := true

lazy val `albion-client` = project
  .in(file("modules/client"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    commonSettings,
    publishSettings,
    Seq(
      libraryDependencies ++= bigquery ++ `cats-effect` ++ `collections-compat` ++ magnolia ++ scalatest ++ runtimeLogging
    )
  )

lazy val examples = project
  .in(file("modules/examples"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    commonSettings,
    publishSettings,
    Seq(
      libraryDependencies ++= runtimeLogging,
      publishArtifact := false
    )
  )
  .dependsOn(`albion-client`)

lazy val root = project
  .in(file("."))
  .configs(IntegrationTest)
  .settings(
    commonSettings,
    publishSettings,
    Seq(
      name := "albion",
      publishArtifact := false,
      crossScalaVersions := Nil
    )
  )
  .aggregate(`albion-client`, examples)
  .dependsOn(`albion-client`)
