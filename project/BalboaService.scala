import Dependencies._
import sbt._
import Keys._

import sbtassembly.Plugin.AssemblyKeys._
import com.socrata.cloudbeessbt.SocrataCloudbeesSbt._
import SocrataSbtKeys._
import sbtassembly.Plugin.{MergeStrategy, PathList}

/**
 * Base Service build settings.  Created this to try and reduce dependency collisions.
 */
object BalboaService {

  /**
   * Base Settings for all BalboaServices.
   */
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly = true) ++ Seq(
    libraryDependencies <++= scalaVersion {libraries(_)}
  )

  def libraries(implicit scalaVersion: String) = Seq(
    // Add dependencies for base Services.
  )

}

/**
 * Java Messaging Service currently using ActiveMQ... Soon to be deprecated and removed.
 */
object BalboaJms {

  lazy val settings: Seq[Setting[_]] = BalboaService.settings ++ Seq(
    mainClass in assembly := Some("com.socrata.balboa.jms.BalboaJms"),
    libraryDependencies <++= scalaVersion {libraries(_)},
    dependenciesSnippet :=
      <xml.group>
        <exclude org="commons-logging" module="commons-logging"/>
      </xml.group>
  )

  def libraries(implicit scalaVersion: String) = Seq(
    activemq
    // Add more dependencies for JMS Service here...
  )
}

/**
 * Kafka message consumption service.
 */
object BalboaKafka {

  val stageConsumer = TaskKey[Unit]("stageForDocker", "Stages a Balboa Kafka Consumer to the docker " +
    "directory.")

  /**
   * Handles staging an assembly Jar for deploying to Docker and apps-marathon.
   */
  val stageConsumerTask = stageConsumer <<= assembly map { (f) =>
    val dockerDir = f.getParentFile.getParentFile.getParentFile / "docker"
    dockerDir.mkdir()
    // TODO Need access to SBT variables at runtime.
    // This might work: https://github.com/ritschwumm/xsbt-reflect
    val c = dockerDir / "balboa-kafka-consumer.jar"
    println("Staging for Docker by copying Balboa Consumer Assembly Jar to " + c)
    IO.copyFile(f, c)
  }

  lazy val settings: Seq[Setting[_]] = BalboaService.settings ++ Seq(
      mainClass in assembly := Some("com.socrata.balboa.common.kafka.BalboaKafkaConsumerCLI"),
      libraryDependencies <++= scalaVersion {libraries(_)},
      parallelExecution in Test := false,
      stageConsumerTask,
      mergeStrategy in assembly := {
        case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
        case "config/config.properties"                    => MergeStrategy.last
        case x =>
          val oldStrategy = (mergeStrategy in assembly).value
          oldStrategy(x)
      }
    )

  def libraries(implicit scalaVersion: String) = BalboaClient.libraries ++
    BalboaKafkaCommon.libraries ++ Seq(
    mockito_test
    // Add more dependencies for Kafka Service here...
  )
}