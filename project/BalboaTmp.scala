import sbt.Keys._
import sbt._
import sbtassembly.Plugin.AssemblyKeys._

object BalboaTmp {
  
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly = true) ++ Seq(
    mainClass in assembly := Some("com.socrata.balboa.tmp.BalboaTmp"),
    libraryDependencies <++= scalaVersion { libraries(_) }
  )

  def libraries(implicit scalaVersion: String) = BalboaCommon.libraries
}