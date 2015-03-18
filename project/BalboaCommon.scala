import Dependencies._
import sbt.Keys._
import sbt._

object BalboaCommon {

  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    libraryDependencies <++= scalaVersion {libraries(_)})

  def libraries(implicit scalaVersion: String) = Seq(
    junit,
    protobuf_java,
    commons_logging,
    jackson_core_asl,
    jackson_mapper_asl
  )
}

object BalboaKafkaCommon {

  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    libraryDependencies <++= scalaVersion {libraries(_)})

  def libraries(implicit scalaVersion: String) = BalboaCommon.libraries ++ Seq(
    kafka,
    kafka_test,
    jopt_simple,
    scalatest
  )
}
