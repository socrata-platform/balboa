import Dependencies._
import sbt.Keys._
import sbt._
import sbtassembly.Plugin.AssemblyKeys._
import sbtassembly.Plugin.{MergeStrategy, PathList}

object BalboaAgent {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly = true) ++ Seq(
    mainClass in assembly := Some("com.socrata.balboa.agent.BalboaAgent"),
    libraryDependencies <++= scalaVersion { libraries(_) }
  )

  def libraries(implicit scalaVersion: String) = BalboaCommon.libraries ++ Seq(
    junit,
    opencsv
  )
}