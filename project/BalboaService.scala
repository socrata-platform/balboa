import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.Calendar

import Dependencies._
import com.socrata.cloudbeessbt.SocrataCloudbeesSbt.SocrataSbtKeys._
import sbt.Keys._
import sbt._
import sbtassembly.Plugin.AssemblyKeys._
import sbtassembly.Plugin.{MergeStrategy, PathList}
import sbtdocker.DockerKeys._
import sbtdocker.Instructions._
import sbtdocker._
import sbtdocker.staging.CopyFile

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

  val appName = settingKey[String]("The application name to use for kafka consumer.  If none is specified then the " +
    "project name is used.")
  val threads = settingKey[Int]("Number of threads to use to consume")
  val topic = settingKey[String]("The topic to subscribe to.")
  val zookeepers = settingKey[List[String]]("Comma separated list of zookeepers")
  val cassandras = settingKey[List[String]]("Comma separated list of Cassandra nodes")

  val configFile =settingKey[File]("Configuration file for this deployment")
  val javaHeapSize = settingKey[String]("Java Heap size")

  /**
   * The command line arguments to pass into the Java Application.
   */
  val commandLineArguments = settingKey[Seq[String]]("Application command line arguments.")

  val javaRunTimeArguments = settingKey[Seq[String]]("Java Runtime configuration variables.")

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
    parallelExecution in Test := false,
    mergeStrategy in assembly := {
      case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
      case "config/config.properties"                    => MergeStrategy.last
      case x =>
        val oldStrategy = (mergeStrategy in assembly).value
        oldStrategy(x)
    },
    stageForDockerTask,
    docker <<= (docker dependsOn assembly),
    appName := name.value,
    threads := 8,
    topic := "",
    zookeepers := Nil,
    cassandras := Nil,
    javaHeapSize := "2048m",
    configFile := baseDirectory.value / "src" / "main" / "resources" / "config" / "deployment_config.properties",
    javaRunTimeArguments := {
      val seq: Seq[String] = javaHeapSize.value match {
        case s: String if !s.trim.isEmpty => Seq(s"-Xmx$s", s"-Xms$s")
        case _ => Seq()
      }
      seq
    },
    commandLineArguments := {
      val z: List[String] = zookeepers.value match {
        case Nil => List("${ZOOKEEPERS}")
        case zs => zs
      }
      val c: List[String] = cassandras.value match {
        case Nil => List("${CASSANDRAS}")
        case cs => cs
      }
      val t = topic.value match {
        case ts: String if !topic.value.trim.isEmpty => ts
        case _ => "${TOPIC}"
      }
      val th = threads.value match {
        case thi: Int if thi > 0 => thi
        case _ => "${THREADS}"
      }
      Seq(s"-appName=${appName.value}",
        s"-topic=$t",
        s"-threads=$th",
        s"-zookeepers=${z.mkString(",")}",
        s"-cassandras=${c.mkString(",")}")
    },
    dockerfile in docker := {
      val artifact = (outputPath in assembly).value
      val srvDirPath = "/srv"
      val jarTargetPath = s"$srvDirPath/${artifact.name}"
      val configTargetPath = "/etc/balboa.properties"

      val instructions = Seq(
        From("socrata/java"),
        Add(CopyFile(configFile.value), configTargetPath),
        Add(CopyFile(artifact), jarTargetPath),
        WorkDir(srvDirPath),
        Cmd.exec(Seq("java") ++ javaRunTimeArguments.value ++ Seq("-jar", jarTargetPath) ++ commandLineArguments.value)
      )
      Dockerfile(instructions)
    },
    buildOptions in docker := BuildOptions(cache = false),
    imageNames in docker := { // The image
      Seq(ImageName(Some("registry.docker.aws-us-west-2-infrastructure.socrata.net:5000"),
        Some("internal"),
        name.value,
        Some(Calendar.getInstance.getTime.getTime.toString))) // TODO Update TAG with build number and SHA
    },
    stageForMarathon := {
      val log = Keys.streams.value.log
      // We need to call docker.value here to invoke a docker build build
      val imageId = dockerBuildAndPush.value
      val imageName = (imageNames in docker).value.head.toString()
      log.info(s"Staging for marathon. Image ID: $imageId Name: $imageName")

      // Write out the docker image to marathon.properties file
      val p = Paths.get("marathon.properties")
      if (Files exists p) {
        Files delete p
      }
      log.info(s"Writing 'DOCKER_IMAGE=$imageName' to 'marathon.properties'")
      Files.write(p, s"DOCKER_IMAGE=$imageName".getBytes(StandardCharsets.UTF_8))
    }
  )

  def libraries(implicit scalaVersion: String) = BalboaClient.libraries ++
    BalboaKafkaCommon.libraries ++ Seq(
    mockito_test
    // Add more dependencies for Kafka Service here...
  )
}

private object ShipDHelper {

  //

}