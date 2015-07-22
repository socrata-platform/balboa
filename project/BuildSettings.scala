import sbt.Keys._
import sbt._

object BuildSettings {
  val buildSettings: Seq[Setting[_]] = Defaults.coreDefaultSettings++ Seq(
    // TODO: enable style build failures
    com.socrata.sbtplugins.StylePlugin.StyleKeys.styleFailOnError in Compile := false,
    // TODO: enable coverage build failures
    scoverage.ScoverageSbtPlugin.ScoverageKeys.coverageFailOnMinimum := false,
    scalaVersion := "2.10.1",
    fork in test := true,
    javaOptions in test += "-Dsocrata.env=test"
    )
  val projectSettings: Seq[Setting[_]] = buildSettings
}
