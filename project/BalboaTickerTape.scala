import sbt.Keys._
import sbt._
import sbtassembly.Plugin.AssemblyKeys._

object BalboaTickerTape {
  
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly = true) ++ Seq(
    mainClass in assembly := Some("com.socrata.balboa.tickertape.BalboaTickerTape"),
    libraryDependencies <++= scalaVersion { libraries(_) }
  )

  def libraries(implicit scalaVersion: String) = BalboaCommon.libraries
}