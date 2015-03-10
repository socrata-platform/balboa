import Dependencies._
import sbt.Keys._
import sbt._
import sbtassembly.Plugin.AssemblyKeys._

object BalboaAdmin {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly = true) ++ Seq(
    mainClass in assembly := Some("com.socrata.balboa.jms.BalboaJms"),
    libraryDependencies <++= scalaVersion { libraries(_) }
  )

  def libraries(implicit scalaVersion: String) = Seq(
    junit,
    opencsv
  )
}
