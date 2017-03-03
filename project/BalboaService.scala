import Dependencies._
import sbt.Keys._
import sbt._
import sbtassembly.AssemblyKeys._
import sbtassembly.MergeStrategy
import scoverage.ScoverageSbtPlugin

/**
 * Base Service build settings.  Created this to try and reduce dependency collisions.
 */
object BalboaService {
  /**
   * Base Settings for all BalboaServices.
   */
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings ++ Seq(
    libraryDependencies <++= scalaVersion {libraries(_)},
    parallelExecution in Test := false,
    assemblyMergeStrategy in assembly := {
      case "config/config.properties" => MergeStrategy.last
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    },
    ScoverageSbtPlugin.ScoverageKeys.coverageMinimum := 0
  )

  def libraries(implicit scalaVersion: String): Seq[ModuleID] = BalboaCommon.libraries ++ Seq(
    // Add dependencies for base Services.
  )
}

/**
 * Java Messaging Service currently using ActiveMQ.
 */
object BalboaJms {
  lazy val settings: Seq[Setting[_]] = BalboaService.settings ++ Seq(
    mainClass in assembly := Some("com.socrata.balboa.jms.BalboaJms"),
    libraryDependencies <++= scalaVersion {libraries(_)},
    ScoverageSbtPlugin.ScoverageKeys.coverageMinimum := 94
  )

  def libraries(implicit scalaVersion: String): Seq[ModuleID] = BalboaService.libraries ++ Seq(
    activemq,
    activemqOpenwire
    // Add more dependencies for JMS Service here...
  )
}

