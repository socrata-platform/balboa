import Dependencies._
import sbt.Keys._
import sbt._
import scoverage.ScoverageSbtPlugin
import org.scalatra.sbt.ScalatraPlugin

object BalboaHttp {
  lazy val settings: Seq[Setting[_]] =
    BuildSettings.projectSettings ++ Seq(
    libraryDependencies <++= scalaVersion {libraries(_)},
    ScoverageSbtPlugin.ScoverageKeys.coverageMinimum := 0
  ) ++ ScalatraPlugin.scalatraFullSettings

  def libraries(implicit scalaVersion: String) = Seq(
    jetty_webapp,
    junit,
    json4s,
    simple_arm,
    scalatra,
    scalatra_json,
    scalatra_metrics,
    newman % "it",
    typesafe_config,
    scalatest
  )
}
