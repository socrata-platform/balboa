import Dependencies._
import sbt.Keys._
import sbt._
import sbtassembly.AssemblyKeys._
import sbtassembly.MergeStrategy

/**
 * Base Service build settings.  Created this to try and reduce dependency collisions.
 */
object BalboaService {
  /**
   * Base Settings for all BalboaServices.
   */
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings ++ Seq(
    libraryDependencies <++= scalaVersion {libraries(_)},
    parallelExecution in Test := false,
    // TODO: disambiguate config.properties files across sub-projects
    assemblyMergeStrategy in assembly := {
      case "config/config.properties" => MergeStrategy.last
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )

  def libraries(implicit scalaVersion: String) = BalboaCommon.libraries ++ Seq(
    // Add dependencies for base Services.
  )
}

/**
 * Java Messaging Service currently using ActiveMQ... Soon to be deprecated and removed.
 */
object BalboaJms {
  lazy val settings: Seq[Setting[_]] = BalboaService.settings ++ Seq(
    mainClass in assembly := Some("com.socrata.balboa.jms.BalboaJms"),
    libraryDependencies <++= scalaVersion {libraries(_)}
  )

  def libraries(implicit scalaVersion: String) = BalboaService.libraries ++ Seq(
    activemq
    // Add more dependencies for JMS Service here...
  )
}

/**
 * Kafka message consumption service.
 */
object BalboaKafka {
  lazy val settings: Seq[Setting[_]] = BalboaService.settings ++ Seq(
    mainClass in assembly := Some("com.socrata.balboa.service.kafka.BalboaKafkaConsumerCLI"),
    libraryDependencies <++= scalaVersion {libraries(_)},
    parallelExecution in Test := false
  )

  def libraries(implicit scalaVersion: String) = BalboaService.libraries ++ BalboaKafkaCommon.libraries
}
