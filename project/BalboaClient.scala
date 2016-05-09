import Dependencies._
import sbt.Keys._
import sbt._
import scoverage.ScoverageSbtPlugin

// The balboa clients must be cross compiled because there are downstream
// consumers that use Scala 2.11.
object BalboaClient {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings ++ Seq(
    libraryDependencies <++= scalaVersion {libraries(_)},
    parallelExecution in Test := false,
    crossScalaVersions := Seq("2.10.6", "2.11.7"),
    ScoverageSbtPlugin.ScoverageKeys.coverageMinimum := 2
  )

  def libraries(implicit scalaVersion: String) = BalboaCommon.libraries ++ Seq(
    socrata_utils,
    simple_arm
  )
}

object BalboaClientJMS {
  lazy val settings: Seq[Setting[_]] = BalboaClient.settings ++ Seq(
    libraryDependencies <++= scalaVersion {libraries(_)},
    ScoverageSbtPlugin.ScoverageKeys.coverageMinimum := 0
  )

  def libraries(implicit scalaVersion: String) = BalboaClient.libraries ++ Seq(
    activemq
  )
}

object BalboaClientKafka {
  lazy val settings: Seq[Setting[_]] = BalboaClient.settings ++ Seq(
    libraryDependencies <++= scalaVersion {libraries(_)},
    parallelExecution in Test := false,
    testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v"),
    ScoverageSbtPlugin.ScoverageKeys.coverageMinimum := 74
  )

  def libraries(implicit scalaVersion: String) = BalboaClient.libraries ++ BalboaKafkaCommon.libraries
}

object BalboaClientDispatcher {
  lazy val settings: Seq[Setting[_]] = BalboaClient.settings ++ Seq(
    libraryDependencies <++= scalaVersion {libraries(_)},
    ScoverageSbtPlugin.ScoverageKeys.coverageMinimum := 70
  )

  def libraries(implicit scalaVersion: String) = BalboaClient.libraries
}
