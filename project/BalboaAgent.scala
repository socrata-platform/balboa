import Dependencies._
import sbt.Keys._
import sbt._
import sbtassembly.Plugin.AssemblyKeys._
import com.typesafe.sbt.packager.docker._
import com.typesafe.sbt.packager.linux._
import com.typesafe.sbt.SbtNativePackager._

object BalboaAgent extends DockerKeys with LinuxKeys {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly = true) ++ Seq(
    mainClass in assembly := Some("com.socrata.balboa.agent.BalboaAgent"),
    libraryDependencies <++= scalaVersion { libraries(_) },
    dockerBaseImage := "socrata/java",
    daemonUser in Docker := "socrata",
    mappings in Docker += file("ship.d/run") -> "/etc/ship.d/run",
    dockerEntrypoint := Seq("/etc/ship.d/run"),
    dockerCommands := dockerCommands.value ++ Seq(ExecCmd("ADD", "etc", "/etc"))
    // TODO Update Ship.d configuration to use a run script.
  )

  def libraries(implicit scalaVersion: String) = BalboaCommon.libraries ++ Seq(
    dropwizard_healthcheck,
    dropwizard_metrics,
    junit,
    scala_test,
    mockito_test
  )
}
