import Dependencies._
import sbt.Keys._
import sbt._
import scoverage.ScoverageSbtPlugin

object BalboaCommon {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings ++ Seq(
    scalaVersion := "2.11.8",
    libraryDependencies <++= scalaVersion {libraries(_)},
    sbtbuildinfo.BuildInfoKeys.buildInfoPackage := "com.socrata.balboa",
    crossScalaVersions := Seq("2.10.6", "2.11.8"),
    ScoverageSbtPlugin.ScoverageKeys.coverageMinimum := 0
  )

  def libraries(implicit scalaVersion: String) = Seq(
    junit,
    protobuf_java,
    mockito_test,
    jackson_core_asl,
    jackson_mapper_asl,
    jopt_simple
  ) ++ balboa_logging
}

object BalboaKafkaCommon {

  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings ++ Seq(
    libraryDependencies <++= scalaVersion {libraries(_)},
    parallelExecution in Test := false,
    ScoverageSbtPlugin.ScoverageKeys.coverageMinimum := 55
  )
  def libraries(implicit scalaVersion: String) = BalboaCommon.libraries ++ Seq(
    kafka,
    kafka_test
  )
}
