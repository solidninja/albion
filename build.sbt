import Dependencies._
import build._

Global / onChangedBuildSource := ReloadOnSourceChanges

lazy val `albion-client` = project
  .in(file("modules/client"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    commonSettings,
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
    Seq(
      libraryDependencies ++= runtimeLogging
    )
  )
  .dependsOn(`albion-client`)

lazy val root = project
  .in(file("."))
  .configs(IntegrationTest)
  .settings(
    commonSettings,
    Seq(
      name := "albion",
      skip in publish := true
    )
  )
  .aggregate(`albion-client`, examples)
  .dependsOn(`albion-client`)
