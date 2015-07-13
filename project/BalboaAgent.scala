import Dependencies._
import com.typesafe.sbt.SbtNativePackager._
import com.typesafe.sbt.packager.debian
import com.typesafe.sbt.packager.debian.DebianPlugin.autoImport._
import sbt.Keys._
import sbt._
import sbtassembly.Plugin.AssemblyKeys._

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
    },
    debianPackageDependencies in debian.DebianPlugin.autoImport.Debian := Seq.empty[String],
    debianPackageRecommends in debian.DebianPlugin.autoImport.Debian := Seq("openjdk-7-jre-headless")
  )

  def libraries(implicit scalaVersion: String) = BalboaCommon.libraries ++ Seq(
    junit,
    scala_test,
    mockito_test
  )
}
