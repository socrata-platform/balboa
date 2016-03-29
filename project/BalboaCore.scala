import Dependencies._
import sbt.Keys._
import sbt._

object BalboaCore {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings ++ Seq(
    libraryDependencies <++= scalaVersion {libraries(_)} ,
    crossScalaVersions := Seq("2.10.6", "2.11.7")
  )

  def libraries(implicit scalaVersion: String) = Seq(
    junit,
    astyanax
  )
}
