import Dependencies._
import sbt.Keys._
import sbt._
import sbtassembly.Plugin.AssemblyKeys._

object BalboaKafka {

  val stageConsumer = TaskKey[Unit]("stageForDocker", "Stages a Balboa Kafka Consumer to the docker " +
    "directory.")

  // Task that handles migrating the assembly jar to docker directory for staging
  val stageConsumerTask = stageConsumer <<= assembly map { (f) =>
    val dockerDir = f.getParentFile.getParentFile.getParentFile / "docker"
    dockerDir.mkdir()
    val c = dockerDir / "balboa-kafka-consumer.jar"
    println("Copying Balboa Consumer Assembly Jar to " + c)
    // TODO Remove current version dynamically and replace with LATEST wtf
    // How do you force the evaluation of Keys.version???
    IO.copyFile(f, c, true)
  }

  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly = true) ++
    Seq(mainClass in assembly :=
      Some("com.socrata.balboa.kafka.BalboaConsumerCLI"),
      libraryDependencies <++= scalaVersion {libraries(_)}
    ) ++ Seq(stageConsumerTask)

  def libraries(implicit scalaVersion: String) = BalboaKafkaCommon.libraries ++ Seq(
    mockito,
    scalatest,
    commons_logging
  )
}