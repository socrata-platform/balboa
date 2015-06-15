import Dependencies._
import sbt.Keys._
import sbt._
import sbtassembly.Plugin.AssemblyKeys._
import com.typesafe.sbt.SbtNativePackager._

object BalboaAgent {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly = true) ++ Seq(
    mainClass in assembly := Some("com.socrata.balboa.agent.BalboaAgent"),
    libraryDependencies <++= scalaVersion { libraries(_) },
    jarName in assembly := s"${name.value}-assembly.jar",
    mappings in Universal :=  {
      val universalMappings = (mappings in Universal).value
      val fatJar = (assembly in Compile).value
      val filtered = universalMappings filter {
        case (file, name) =>  ! name.endsWith(".jar")
      }
      filtered :+ (fatJar -> ("bin/" + fatJar.getName))
    }
  )

  def libraries(implicit scalaVersion: String) = BalboaCommon.libraries ++ Seq(
    junit,
    opencsv
  )
}