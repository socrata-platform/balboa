package com.socrata.balboa.kafka

import java.io.{IOException, File, FileReader}
import java.util.Properties
import java.util.concurrent.Executors

import com.socrata.balboa.kafka.BalboaConfig
import com.socrata.balboa.kafka.consumer.{DSConsumer, TestConsumer}
import com.socrata.balboa.metrics.config.Configuration
import com.socrata.balboa.metrics.data.{DataStoreFactory, DataStore}
import kafka.consumer.ConsumerConfig

/**
 * Created by Michael Hotan, michael.hotan@socrata.com on 2/17/15.
 *
 * Balboa Kafka Main Application:
 *
 * Entry point for Balboa Kafka Consumer.  This application starts a service that listens
 * on a configurable list of ZooKeeper Host Ports.  This service currently listens for all published Tenant Metrics.
 *
 */
object Main extends App {

  /**
   * Parse handles command line configuration.
   */
  val parser = new scopt.OptionParser[BalboaConfig]("balboa-kafka") {
    opt[File]('f', "configFile") action { (x,c) => c.copy(configFile = x) } validate {
      x => if (x.exists()) success else failure("Configuration does not exist") } text("Kafka configuration file")
    opt[String]('z', "zookeepers") action { (x,c) => c.copy(zookeepers = x.trim)} validate { x => if (x.split(',').length
      > 0) success else failure("Option --zookeepers must have 1 or more host:port names") } text("Comma separated " +
      "list of zookeeper host:port")
    opt[String]('g', "groupid") action { (x,c) => c.copy(groupId = x.trim) } text("Consumer group id")
    opt[String]('t', "topic") action {(x,c) => c.copy(topic = x)} text("The topic to consume")
    opt[Int]('n', "threads") action {(x,c) => c.copy(threads = x)} text("Number of threads to use") validate(x => if
    (x > 0) success else failure("Must have positive number of threads"))
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

      // Kafka Consumer configuration.
      val consumerConfig: ConsumerConfig = new ConsumerConfig(props)
      val threads: Int = props.getProperty("balboa.kafka.topic.partitions", "1").toInt
      val topic: String = props.getProperty("balboa.kafka.topic")

      val cg: BalboaConsumerGroup = BalboaConsumerGroup.init(consumerConfig, topic)
      BalboaConsumerGroup.getStreams(cg, threads) match {
        case Some(streams) =>
          // For each available stream create a consumer to ingest the stream.
          val executor = Executors.newFixedThreadPool(threads)
          streams.foreach(stream => executor.submit(DSConsumer(stream, DataStoreFactory.get())))
        case None => Console.println("No Available Stream to print")
      }

    case None =>
      // TODO More descriptive error and harder penalty for configuration failure.
      sys.error("Illegal Configuration")
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
    properties.setProperty("balboa.kafka.topic.partitions", "8")
    // This would be the appropiate place to set default values.
    properties
  }

}

/**
 * Private class that represents Balboa Kafka Configuration.  This configuration is just used for
 * interpreting command line arguments.  It provides a nice convention for extending the interface.
 *
 * @param configFile Configuration file.
 * @param zookeepers Comma separated list of zookeepers with port
 * @param groupId The Group this consumer belongs too.
 * @param topic The Topic to list to.
 */
sealed case class BalboaConfig( configFile: File = null,
                                zookeepers: String = null,
                                groupId: String = null,
                                topic: String = null,
                                threads: Int = 1)
