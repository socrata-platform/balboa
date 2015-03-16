package com.socrata.balboa.common.kafka

import java.io.{File, FileReader, IOException}
import java.util.Properties

import com.socrata.balboa.common.kafka.consumer.BalboaConsumerGroup
import com.socrata.balboa.metrics.config.Configuration
import com.socrata.balboa.metrics.data.{DataStore, DataStoreFactory}
import kafka.consumer.ConsumerConfig
import org.apache.commons.logging.LogFactory

/**
 * Balboa Kafka Main Application:
 *
 * Entry point for Balboa Kafka Consumer.  This application starts a service that listens
 * on a configurable list of ZooKeeper Host Ports.  This service currently listens for all published Tenant Metrics.
 */
object BalboaKafkaConsumerCLI extends App {

  private val Log = LogFactory.getLog("Balboa Kafka Consumer")

  val DEFAULT_NUM_PARTITIONS = 8
  val DEFAULT_CONSUMER_WAITTIME = 1000

  /**
   * Parse handles command line configuration.
   */
  val parser = new scopt.OptionParser[BalboaConfig]("balboa-kafka") {

    // Configuration file
    opt[File]('f', "configFile") action { (x, c) => c.copy(configFile = x)} validate { x => if (x.exists()) success
    else failure("Configuration does not exist")} text "Kafka configuration file"

    // Zookeeper servers.
    opt[String]('z', "zookeepers") action { (x,c) => c.copy(zookeepers = x.trim)} validate { x => if (x.split(',').length
      > 0) success else failure("Option --zookeepers must have 1 or more host:port names") } text("Comma separated " +
      "list of zookeeper host:port")

    // The Kafka group id.
    opt[String]('g', "groupid") action { (x,c) => c.copy(groupId = x.trim) } text "Consumer group id"

    // The Kafka Topic to listen for
    opt[String]('t', "topic") action {(x,c) => c.copy(topic = x)} text "The topic to consume"

    // Number of threads to spawn to start ingesting data.
    opt[Int]('n', "threads") action {(x,c) => c.copy(threads = x)} text "Number of threads to use" validate (x => if
    (x > 0) success else failure("Must have positive number of threads"))

    // Wait time for a consumer thread
    opt[Long]('w', "waittime") action {(x,c) => c.copy(waitTime = x)} text "Time a single consumer is allotted to " +
      s"wait before continuing to consume messages. Default: $DEFAULT_CONSUMER_WAITTIME ms" validate (x => if (x > 0)
      success else failure("Must have positive wait time"))
  }

  // Retrieve the default data store.
  val ds: DataStore = DataStoreFactory.get

  // Actually parse the command line argument
  parser.parse(args, BalboaConfig()) match {
    case Some(config) =>

      val props: Properties = properties(config) match {
        case Right(properties) => properties
        case Left(message) => sys.error(s"Unable to create properties: $message")
      }
      val consumerConfig: ConsumerConfig = new ConsumerConfig(props)
      val threads: Int = props.getProperty("balboa.kafka.topic.partitions", s"$DEFAULT_NUM_PARTITIONS").toInt
      val topic: String = props.getProperty("balboa.kafka.topic")

      BalboaConsumerGroup.create(consumerConfig, topic, threads, DataStoreFactory.get(), config.waitTime) match {
        case Some(g: BalboaConsumerGroup) => g.run()
        case _ => Log.fatal("Unable to initiate consumer.  Check your configuration and usage.  " +
          "Check number of threads: $threads are positive and $topic is a nonnull nor empty word")
      }

    case None =>
      // Usage should be printed if there is an error
      // Stop the application before continuing any further
      throw new IllegalArgumentException("Unable to parse command line arguments.")
  }

  /**
   * Given a BalboaConfiguration instance create a respective Properties file
   *
   * @param bc Balboa Configuration
   * @return Either properties file or error message.
   */
  def properties(bc: BalboaConfig) : Either[String, Properties] = {
    (bc.configFile, bc.zookeepers, bc.groupId, bc.topic, bc.threads) match {
      case (f: File, _, _, _, _) => propertiesFromFile(f)
      case (_,z: String, g: String, t: String, n: Int) => Right(propertiesFromArguments(z,g,t,n))
      case (null, null, null, null, _) => Right(Configuration.get) // Use developer configurations
      case _=> Left("Unable to configure properties, please see usage.")
    }
  }

  /**
   * Return the properties created from an existing properties file.
   *
   * @param file Configuration file
   * @return The Properties generated from a file.
   */
  def propertiesFromFile(file: File): Either[String, Properties] = file match {
    case _ if !file.exists() => Left("File not found: " + file.getAbsolutePath)
    case _ => val properties = new Properties()
      try {
        properties.load(new FileReader(file))
        Right(properties)
      } catch { case e: IOException => Left(e.getMessage) }
  }

  /**
   * Creates bare bones properties configuration given command line arguments.
   *
   * @param zookeepers Comma seperated list of zookeeper host.
   * @param groupId the Group Id this consumer belongs in
   * @return The Property instance for these zookeepers and groupid
   */
  def propertiesFromArguments(zookeepers: String, groupId: String, topic: String, threads: Int): Properties = {
    val properties = new Properties()
    properties.setProperty("zookeeper.connect", zookeepers)
    properties.setProperty("group.id", groupId)
    properties.setProperty("balboa.kafka.topic", topic)
    properties.setProperty("balboa.kafka.topic.partitions", threads.toString)
    // This would be the appropiate place to set default values.
    properties
  }

  /**
   * Private class that represents Balboa Kafka Configuration.  This configuration is just used for
   * interpreting command line arguments.  It provides a nice convention for extending the interface.
   *
   * @param configFile Configuration file.
   * @param zookeepers Comma separated list of zookeepers with port
   * @param groupId The Group ID this consumer belongs too.
   * @param topic Kafka Topic to list for.
   * @param waitTime The time a thread is allotted to wait for a consumer
   */
  case class BalboaConfig( configFile: File = null,
                           zookeepers: String = null,
                           groupId: String = null,
                           topic: String = null,
                           threads: Int = DEFAULT_NUM_PARTITIONS,
                           waitTime: Long = DEFAULT_CONSUMER_WAITTIME)

}


