import Dependencies._
import sbt.Keys._
import sbt._

object BalboaHttp {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings ++ Seq(
    libraryDependencies <++= scalaVersion {libraries(_)}
  )

  def libraries(implicit scalaVersion: String) = Seq(
    junit,
    rojoma_json,
    simple_arm,
    socrata_http
  )
}
