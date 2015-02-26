import sbt.Keys._
import sbt._

import sbtassembly.Plugin.AssemblyKeys._

object BalboaKafka {

  val stageDocker = TaskKey[Unit]("stageDocker", "Copies assembly jar to docker directory")

  // Task that handles migrating the assembly jar to docker directory for staging
  val stageDockerTask = stageDocker <<= assembly map { (f) =>
    println("Staging file for Docker: " + Keys.version)
    // TODO Remove current version and replace with LATEST wtf why can't I do this
    val dockerDir = new File("docker")
    dockerDir.mkdir()
    IO.copyFile(f, dockerDir / f.getName)
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
    ) ++ Seq(stageDockerTask)
}