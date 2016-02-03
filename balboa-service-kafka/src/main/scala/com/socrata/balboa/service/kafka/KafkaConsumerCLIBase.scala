package com.socrata.balboa.service.kafka

import java.io.{File, FileNotFoundException}
import java.util.Properties

import com.socrata.balboa.metrics.config.Configuration
import com.socrata.balboa.service.kafka.consumer.KafkaConsumerGroupComponent
import joptsimple.{OptionParser, OptionSet}
import kafka.utils.VerifiableProperties
import org.slf4j.LoggerFactory

/**
  * Base Trait for running a consumer group through a command line application.
  */
trait KafkaConsumerCLIBase[K,M] extends App {

  private val Log = LoggerFactory.getLogger(this.getClass)

  private val APPLICATION_NAME_KEY = "balboa.kafka.application.name"
  private val ZOOKEEPER_PROPERTY_KEY = "zookeeper.connect"
  private val GROUPID_PROPERTY_KEY = "group.id"
  private val TOPIC_PROPERTY_KEY = "balboa.kafka.topic"
  private val TOPIC_PARTITIONS_PROPERTY_KEY = "balboa.kafka.topic.partitions"

  private val LOCAL_ZOOKEEPER = "localhost:2181"

  //////////////////////////////////////////////////////////////////////////////////////////
  /////// Running the Application

  Log.info("Creating Consumer Group")
  val g = consumerGroup()
  Log.info("Completed Creating Consumer Group")

  // Add the shutdown hook.
  Log.info("Adding Shutdown hook")
  scala.sys.addShutdownHook(try {
    g.stop()
  } catch {
    case e: Exception => Log.warn("Unable to stop consumer group. ", e)
    case t: Throwable => Log.warn(s"Unknown error while stopping consumer group.", t)
  })

  Log.info(s"Starting $g...")
  g.start()

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
    val props = new Properties()
    if (set.has("configFile")) {
      // If there is a specific configuration file over
      val collectionOfFiles = set.valuesOf("configFile")
      collectionOfFiles.get(0) match {
        case f: File => props.putAll(propertiesFromFile(f))
        case x => throw new IllegalArgumentException("JOpt fail to return file.")
      }
    } else {
      props.putAll(Configuration.get())
      set.valueOf("zookeepers") match {
        case z: String =>
          props.setProperty(ZOOKEEPER_PROPERTY_KEY, z)
        case _ => // NOOP
      }
      set.valueOf("appName") match {
        case a: String =>
          props.setProperty(GROUPID_PROPERTY_KEY, a)
        case _ => // NOOP
      }
      set.valueOf("topic") match {
        case t: String =>
          props.setProperty(TOPIC_PROPERTY_KEY, t)
        case _ => // NOOP
      }
      set.valueOf("threads") match {
        case p: Integer =>
          props.setProperty(TOPIC_PARTITIONS_PROPERTY_KEY, p.toString)
        case _ => // NOOP
      }
    }
    props
  }

  /**
    * Override this method if you want to introduce a new command line argument.  See [[BalboaKafkaConsumerCLI]] as an
    * example.
    *
    * @return The [[OptionParser]] that handles parsing command line input.
    */
  protected def optionParser(): OptionParser = {
    val optParser: OptionParser = new OptionParser()
    // Can use a single configuration file for all command line application.
    val confOpt = optParser.accepts("configFile", "Configuration File.")
      .withRequiredArg()
      .ofType(classOf[File])
    optParser.accepts("zookeepers", "Comma separated list of Zookeeper server:port's")
      .requiredUnless(confOpt)
      .withRequiredArg()
      .ofType(classOf[String])
      .defaultsTo(LOCAL_ZOOKEEPER)
    optParser.accepts("appName", "Application Name")
      .requiredUnless(confOpt)
      .withRequiredArg()
      .ofType(classOf[String])
      .describedAs("A name associated with this consumer.  This will be used as a Kafka \"group id\".")
    optParser.accepts("topic", "Kafka topic to subscribe to.")
      .requiredUnless(confOpt)
      .withRequiredArg()
      .ofType(classOf[String])
    optParser.accepts("threads", "Number of consumer threads for this Consumer application.")
      .requiredUnless(confOpt)
      .withRequiredArg()
      .ofType(classOf[Int])
      .defaultsTo(1)
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
    case _: File if !file.exists() => throw new FileNotFoundException(s"File $file not found")
    case _ =>
      // Cheat and override any current System property configuration
      System.setProperty("balboa.config", file.getAbsolutePath)
      Configuration.get()
  }

}
