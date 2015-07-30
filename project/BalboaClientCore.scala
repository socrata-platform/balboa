import Dependencies._
import com.socrata.cloudbeessbt.SocrataCloudbeesSbt.SocrataSbtKeys._
import sbt.Keys._
import sbt._

object BalboaClientCore {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    crossScalaVersions := Seq("2.8.2", "2.10.4", "2.11.6"),
    libraryDependencies <++= scalaVersion {libraries(_)},
    parallelExecution in Test := false,
    dependenciesSnippet :=
      <xml.group>
        <conflict org="com.socrata" manager="latest-compatible" />
        <conflict org="com.rojoma" manager="latest-compatible" />
      </xml.group>
  )

  def libraries(implicit scalaVersion: String) = BalboaCommon.libraries ++ Seq(
    socrata_utils,
    simple_arm
  )
}

object BalboaClientJMS {
  lazy val settings: Seq[Setting[_]] = BalboaClientCore.settings ++ Seq(
    libraryDependencies <++= scalaVersion {libraries(_)}
  )

  def libraries(implicit scalaVersion: String) = BalboaClientCore.libraries ++ Seq(
    activemq
  )
}

object BalboaClientKafka {
  lazy val settings: Seq[Setting[_]] = BalboaClientCore.settings ++ Seq(
    libraryDependencies <++= scalaVersion {libraries(_)},
    parallelExecution in Test := false
  )

  def libraries(implicit scalaVersion: String) = BalboaClientCore.libraries
}

object BalboaClientDispatcher {
  lazy val settings: Seq[Setting[_]] = BalboaClientCore.settings ++ Seq(
    libraryDependencies <++= scalaVersion {libraries(_)}
  )

  def libraries(implicit scalaVersion: String) = BalboaClientCore.libraries
}
