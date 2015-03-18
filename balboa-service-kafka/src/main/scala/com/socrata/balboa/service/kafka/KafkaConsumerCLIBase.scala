package com.socrata.balboa.service.kafka

import java.io.{FileNotFoundException, File, FileReader, IOException}
import java.util
import java.util.Properties

import com.socrata.balboa.metrics.config.Configuration
import com.socrata.balboa.service.kafka.consumer.KafkaConsumerGroupComponent
import joptsimple.{OptionParser, OptionSet}
import kafka.utils.VerifiableProperties
import org.apache.commons.logging.LogFactory

/**
 * Base Trait for running a consumer group through a command line application.
 */
trait KafkaConsumerCLIBase[K,M] extends App {

  protected val Log = LogFactory.getLog(this.getClass)

  private val APPLICATION_NAME_KEY = "balboa.kafka.application.name"
  private val ZOOKEEPER_PROPERTY_KEY = "zookeeper.connect"
  private val GROUPID_PROPERTY_KEY = "group.id"
  private val TOPIC_PROPERTY_KEY = "balboa.kafka.topic"
  private val TOPIC_PARTITIONS_PROPERTY_KEY = "balboa.kafka.topic.partitions"

  //////////////////////////////////////////////////////////////////////////////////////////
  /////// Running the Application

  override def main(args: Array[String]): Unit = {
    super.main(args)
    val g = consumerGroup()
    Log.info(s"Starting $g...")
    g.start()
  }

  //////////////////////////////////////////////////////////////////////////////////////////
  /////// Abstract Members

  /**
   * Create a consumer group to Run.
   *
   * @return The Consumer Group Component that consumes messages.
   */
  protected def consumerGroup(): KafkaConsumerGroupComponent[K,M]

  //////////////////////////////////////////////////////////////////////////////////////////
  /////// Properties

  lazy val zookeepers: String = properties.getString(ZOOKEEPER_PROPERTY_KEY)
  lazy val groupID: String = properties.getString(GROUPID_PROPERTY_KEY)
  lazy val topic: String = properties.getString(TOPIC_PROPERTY_KEY)
  lazy val partitions: Int = properties.getInt(TOPIC_PARTITIONS_PROPERTY_KEY)

  /**
   * The properties for this consumer group application.
   */
  lazy val properties: VerifiableProperties = new VerifiableProperties(getProperties(args))

  /**
   * Parses command line arguments for properties.
   * <br>
   *   Override this method if you introduced a new command line argument and need to validate or extract it.  See
   *   [[BalboaKafkaConsumerCLI]] as an example.
   *
   * @param args Command line arguments
   * @return Properties for this consumer.
   */
  protected def getProperties(args: Array[String]): Properties = {
    val set: OptionSet = optionParser().parse(args : _*)
    if (set.has("configFile")) {
      val collectionOfFiles = set.valuesOf("configFile")
      collectionOfFiles.get(0) match {
        case f: File => propertiesFromFile(f)
        case x => throw new ClassCastException("JOpt fail to return file.")
      }
    } else {
      val zookeepers = set.valueOf("zookeepers") match {
        case z: String => z
        case _ => throw new ClassCastException()
      }
      val appName = set.valueOf("appName") match {
        case a: String => a
        case _ => throw new ClassCastException()
      }
      val topic = set.valueOf("topic") match {
        case t: String => t
        case _ => throw new ClassCastException()
      }
      val partitions = set.valueOf("partitions") match {
        case p: Integer => p
        case _ => throw new ClassCastException()
      }
      propertiesFromArguments(zookeepers, appName, topic, partitions)
    }
  }

  /**
   * Override this method if you want to introduce a new command line argument.  See [[BalboaKafkaConsumerCLI]] as an
   * example.
   *
   * @return The [[OptionParser]] that handles parsing command line input.
   */
  protected def optionParser(): OptionParser = {
    val optParser: OptionParser = new OptionParser()
    val confOpt = optParser.accepts("configFile", "Configuration File.").withRequiredArg().ofType(classOf[File])
    optParser.accepts("zookeepers", "Comma separated list of Zookeeper server:port(s)").requiredUnless(confOpt)
      .withRequiredArg().ofType(classOf[String]).defaultsTo("localhost:2181")
    optParser.accepts("appName", "Application Name").requiredUnless(confOpt).withRequiredArg()
      .ofType(classOf[String]).defaultsTo(this.getClass.getSimpleName).describedAs("A name associated with this " +
      "consumer name.  This will be used as a Kafka \"group id\"")
    optParser.accepts("topic", "Kafka topic to subscribe to.").requiredUnless(confOpt)
      .withRequiredArg().ofType(classOf[String])
    optParser.accepts("partitions", "Partitions for the given topic").requiredUnless(confOpt)
      .withRequiredArg().ofType(classOf[Int]).defaultsTo(1)
    optParser
  }

  //////////////////////////////////////////////////////////////////////////////////////////
  /////// Private Helper methods.

  /**
   * Return the properties created from an existing properties file.
   *
   * @param file Configuration file
   * @return The Properties generated from a file.
   */
  private def propertiesFromFile(file: File): Properties = file match {
    case _ if !file.exists() => throw new FileNotFoundException(s"File $file not found")
    case _ =>
      // Cheat at override any current System property configuration
      System.setProperty("balboa.config", file.getAbsolutePath)
      Configuration.get()
  }


  /**
   * Creates bare bones properties configuration given command line arguments.
   *
   * @param zookeepers Comma separated list of zookeeper host.
   * @param groupId the Group Id this consumer belongs in
   * @return The Property instance for these zookeepers and groupid
   */
  private def propertiesFromArguments(zookeepers: String, groupId: String, topic: String, threads: Int): Properties = {
    val properties = new Properties()
    properties.setProperty(ZOOKEEPER_PROPERTY_KEY, zookeepers)
    properties.setProperty(GROUPID_PROPERTY_KEY, groupId)
    properties.setProperty(TOPIC_PROPERTY_KEY, topic)
    properties.setProperty(TOPIC_PARTITIONS_PROPERTY_KEY, threads.toString)
    // This would be the appropriate place to set default values.
    properties
  }

}
