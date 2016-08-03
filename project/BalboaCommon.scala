import Dependencies._
import sbt.Keys._
import sbt._
import scoverage.ScoverageSbtPlugin

object BalboaCommon {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings ++ Seq(
    libraryDependencies <++= scalaVersion {libraries(_)},
    sbtbuildinfo.BuildInfoKeys.buildInfoPackage := "com.socrata.balboa",
    crossScalaVersions := Seq("2.10.6", "2.11.8"),
    ScoverageSbtPlugin.ScoverageKeys.coverageMinimum := 0
  )

  def libraries(implicit scalaVersion: String) = Seq(
    // SLF4J is used directly here instead of scala-logging to allow for cross-compilation to 2.10
    log4j,
    slf4j_log4j,
    junit,
    protobuf_java,
    mockito_test,
    jackson_core_asl,
    jackson_mapper_asl,
    jopt_simple,
    json4s
  )
}
