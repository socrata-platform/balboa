import Dependencies._
import sbt.Keys._
import sbt._

object BalboaClient {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings ++ Seq(
    crossScalaVersions := Seq("2.8.2", "2.10.4", "2.11.6"),
    libraryDependencies <++= scalaVersion {libraries(_)},
    parallelExecution in Test := false
  )

  def libraries(implicit scalaVersion: String) = BalboaCommon.libraries ++ Seq(
    socrata_utils,
    simple_arm
  )
}

object BalboaClientJMS {
  lazy val settings: Seq[Setting[_]] = BalboaClient.settings ++ Seq(
    libraryDependencies <++= scalaVersion {libraries(_)}
  )

  def libraries(implicit scalaVersion: String) = BalboaClient.libraries ++ Seq(
    activemq
  )
}

object BalboaClientKafka {
  lazy val settings: Seq[Setting[_]] = BalboaClient.settings ++ Seq(
    libraryDependencies <++= scalaVersion {libraries(_)},
    parallelExecution in Test := false,
    testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v")
  )

  def libraries(implicit scalaVersion: String) = BalboaClient.libraries ++ BalboaKafkaCommon.libraries
}

object BalboaClientDispatcher {
  lazy val settings: Seq[Setting[_]] = BalboaClient.settings ++ Seq(
    libraryDependencies <++= scalaVersion {libraries(_)}
  )

  def libraries(implicit scalaVersion: String) = BalboaClient.libraries
}
