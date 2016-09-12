import Dependencies._
import com.typesafe.sbt.SbtNativePackager._
import com.typesafe.sbt.packager.docker._
import com.typesafe.sbt.packager.linux._
import sbt.Keys._
import sbt._
import scoverage.ScoverageSbtPlugin

object BalboaAgent extends DockerKeys with LinuxKeys {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings ++ Seq(
    mainClass in sbtassembly.AssemblyKeys.assembly := Some("com.socrata.balboa.agent.BalboaAgent"),
    libraryDependencies <++= scalaVersion { libraries(_) },
    dockerBaseImage := "socrata/java",
    daemonUser in Docker := "socrata",
    mappings in Docker += file("balboa-agent/ship.d/run") -> "/etc/ship.d/run",
    dockerEntrypoint := Seq("/etc/ship.d/run"),
    dockerCommands := dockerCommands.value ++ Seq(ExecCmd("ADD", "etc", "/etc")),
    ScoverageSbtPlugin.ScoverageKeys.coverageMinimum := 61,
    ScoverageSbtPlugin.ScoverageKeys.coverageExcludedPackages := "<empty>;.*\\.balboa\\.agent\\.BalboaAgent",
    com.socrata.sbtplugins.StylePlugin.StyleKeys.styleFailOnError in Compile := true
  )

  def libraries(implicit scalaVersion: String) = BalboaCommon.libraries ++ Seq(
    commons_logging,
    dropwizard_metrics,
    dropwizard_servlets,
    junit,
    json4s,
    newman,
    scalatest,
    json4s
  ) ++ balboa_logging
}
