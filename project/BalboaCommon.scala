import Dependencies._
import sbt.Keys._
import sbt._
import sbtassembly.Plugin.AssemblyKeys._
import sbtassembly.Plugin.{MergeStrategy, PathList}

object BalboaCommon {

  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    libraryDependencies <++= scalaVersion {libraries(_)}
  )

  def libraries(implicit scalaVersion: String) = Seq(
    junit,
    protobuf_java,
    scala_test,
    mockito_test,
    jackson_core_asl,
    jackson_mapper_asl,
    jopt_simple
  ) ++ balboa_logging
}

object BalboaKafkaCommon {

  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    libraryDependencies <++= scalaVersion {libraries(_)},
    parallelExecution in Test := false
  )
  def libraries(implicit scalaVersion: String) = BalboaCommon.libraries ++ Seq(
    kafka,
    kafka_test
  )
}
