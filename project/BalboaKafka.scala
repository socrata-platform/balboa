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
    println("Copying file to " + c)
    // TODO Remove current version dynamically and replace with LATEST wtf
    // How do you force the evaluation of Keys.version???
    IO.copyFile(f, c)
  }

  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly = true) ++
    Seq(mainClass in assembly :=
      Some("com.socrata.balboa.kafka.Main"),
      libraryDependencies ++= Seq(
        "org.mockito" % "mockito-core" % "1.+" % "test",
        "org.scalatest" %% "scalatest" % "2.2.4" % "test",
        "org.apache.kafka" %% "kafka" % "0.8.2.0",
        "commons-logging" % "commons-logging" % "1.1.1",
        "com.github.scopt" %% "scopt" % "3.3.0"
      )
    ) ++ Seq(stageConsumerTask)
}