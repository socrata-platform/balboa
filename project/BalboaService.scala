import Dependencies._
import com.socrata.cloudbeessbt.SocrataCloudbeesSbt.SocrataSbtKeys._
import sbt.Keys._
import sbt._
import sbtassembly.Plugin.AssemblyKeys._
import sbtassembly.Plugin.MergeStrategy

/**
 * Base Service build settings.  Created this to try and reduce dependency collisions.
 */
object BalboaService {

  /**
   * Base Settings for all BalboaServices.
   */
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly = true) ++ Seq(
    libraryDependencies <++= scalaVersion {libraries(_)},
    mergeStrategy in assembly := {
      case "config/config.properties" => MergeStrategy.last
      case x =>
        val oldStrategy = (mergeStrategy in assembly).value
        oldStrategy(x)
    }
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

  val stageForDocker = taskKey[Unit]("Stages a Balboa Kafka Consumer to the docker " +
    "directory.")

  /**
   * Stage the assembly jar for use in a docker script.
   */
  val stageForDockerTask = stageForDocker := {
    val log = Keys.streams.value.log
    val assemblyJar = assembly.value
    val dockerDir = baseDirectory.value / "docker"
    dockerDir.mkdir()
    val copy = dockerDir / s"${name.value}-LATEST.jar"
    log.info(s"Staging assembly jar at ${copy}")
    IO.copyFile(assemblyJar, copy)
  }

  val stageForMarathon = taskKey[Unit]("Stages the docker image for a follow up marathon deployment.")

  lazy val settings: Seq[Setting[_]] = BalboaService.settings ++ Seq(
    mainClass in assembly := Some("com.socrata.balboa.service.kafka.BalboaKafkaConsumerCLI"),
    libraryDependencies <++= scalaVersion {libraries(_)},
    parallelExecution in Test := false
  )

  def libraries(implicit scalaVersion: String) = BalboaService.libraries ++ BalboaKafkaCommon.libraries
}
