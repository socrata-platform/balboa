import Dependencies._
import com.socrata.cloudbeessbt.SocrataCloudbeesSbt.SocrataSbtKeys._
import sbt.Keys._
import sbt._

object BalboaClient {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    crossScalaVersions := Seq("2.8.2", "2.10.1"),
    libraryDependencies <++= scalaVersion {libraries(_)},
    dependenciesSnippet :=
      <xml.group>
        <conflict org="com.socrata" manager="latest-compatible" />
        <conflict org="com.rojoma" manager="latest-compatible" />
      </xml.group>
  )

  def libraries(implicit scalaVersion: String) = Seq(
    junit,
    scalatest,
    socrata_utils,
    simple_arm,
    log4j
  )
}

object BalboaClientJMS {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    libraryDependencies <++= scalaVersion {libraries(_)},
    dependenciesSnippet :=
      <xml.group>
        <conflict org="com.socrata" manager="latest-compatible" />
        <conflict org="com.rojoma" manager="latest-compatible" />
     </xml.group>
  )

  def libraries(implicit scalaVersion: String) = BalboaClient.libraries ++ Seq(
    junit,
    scalatest,
    activemq
  )
}

object BalboaClientKafka {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    libraryDependencies <++= scalaVersion {libraries(_)},
    dependenciesSnippet :=
      <xml.group>
        <conflict org="com.socrata" manager="latest-compatible" />
        <conflict org="com.rojoma" manager="latest-compatible" />
      </xml.group>
  )

  def libraries(implicit scalaVersion: String) = BalboaClient.libraries ++ BalboaKafkaCommon.libraries
}
