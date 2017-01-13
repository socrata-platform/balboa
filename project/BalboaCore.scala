import Dependencies._
import sbt.Keys._
import sbt._
import scoverage.ScoverageSbtPlugin

object BalboaCore {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings ++ Seq(
    libraryDependencies <++= scalaVersion {libraries(_)} ,
    crossScalaVersions := Seq("2.10.6", "2.11.8"),
    ScoverageSbtPlugin.ScoverageKeys.coverageMinimum := 0
  )

  def libraries(implicit scalaVersion: String): Seq[ModuleID] = Seq(
    cassandra_driver_core,
    cassandra_driver_mapping,
    cassandra_driver_extras,
    log4j,
    mockito_test,
    java_nullable_annotation,
    junit
  ) ++ balboa_logging
}
