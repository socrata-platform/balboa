import Dependencies._
import sbt.Keys._
import sbt._
import scoverage.ScoverageSbtPlugin

object BalboaHttp {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings ++ Seq(
    libraryDependencies <++= scalaVersion {libraries(_)},
    ScoverageSbtPlugin.ScoverageKeys.coverageMinimum := 0
  )

  // balboa-http can not be cross compiled because of the dependency on
  // socrata-http.
  def libraries(implicit scalaVersion: String) = Seq(
    junit,
    rojoma_json,
    simple_arm,
    socrata_http,
    newman % "it",
    typesafe_config,
    scalatest,
    json4s
  )
}
