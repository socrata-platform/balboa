import Dependencies._
import sbt.Keys._
import sbt._
import scoverage.ScoverageSbtPlugin
//import com.earldouglas.xwp.JettyPlugin
import org.scalatra.sbt.ScalatraPlugin

object BalboaHttp {
  lazy val settings: Seq[Setting[_]] =
    //(project in file(".")).enablePlugins(JettyPlugin).settings ++
    BuildSettings.projectSettings ++ Seq(
    libraryDependencies <++= scalaVersion {libraries(_)},
    ScoverageSbtPlugin.ScoverageKeys.coverageMinimum := 0
  ) ++ ScalatraPlugin.scalatraFullSettings

  // balboa-http can not be cross compiled because of the dependency on
  // socrata-http.
  def libraries(implicit scalaVersion: String) = Seq(
    jetty_webapp,
    junit,
    json4s,
    rojoma_json,
    simple_arm,
    scalatra,
    scalatra_json,
    newman % "it",
    typesafe_config,
    scalatest,
    json4s
  )
}
