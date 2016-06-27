import Dependencies._
import sbt.Keys._
import sbt._
import scoverage.ScoverageSbtPlugin

object BalboaAdmin {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings ++ Seq(
    scalaVersion := "2.11.8",
    mainClass in sbtassembly.AssemblyKeys.assembly := Some("com.socrata.balboa.admin.BalboaAdmin"),
    libraryDependencies <++= scalaVersion { libraries(_) },
    ScoverageSbtPlugin.ScoverageKeys.coverageMinimum := 100
  )

  def libraries(implicit scalaVersion: String) = Seq(
    junit,
    opencsv
  )
}
