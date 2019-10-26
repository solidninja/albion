import Dependencies._
import build._

Global / onChangedBuildSource := ReloadOnSourceChanges

lazy val albion = Project(
  id = "albion",
  base = file(".")
).settings(
  commonSettings,
  Seq(
    libraryDependencies ++= bigquery ++ `cats-effect` ++ fs2 ++ magnolia ++ scalatest ++ runtimeLogging
  )
)
