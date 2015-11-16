import Dependencies._
import sbt.Keys._
import sbt._

object BalboaAdmin {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings ++ Seq(
    mainClass in sbtassembly.AssemblyKeys.assembly := Some("com.socrata.balboa.admin.BalboaAdmin"),
    libraryDependencies <++= scalaVersion { libraries(_) },
    mainClass in Compile := Some("com.socrata.balboa.admin.BalboaAdmin")
  )

  def libraries(implicit scalaVersion: String) = Seq(
    junit,
    opencsv,
    scopt,
    typesafe_config
  )
}
