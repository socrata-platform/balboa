package com.socrata.balboa.agent

import java.io.File

import com.socrata.balboa.common.logging.BalboaLogging
import com.socrata.metrics.MetricQueue
import joptsimple.{OptionParser, OptionSet}

object CLIParamKeys {

  lazy val dataDir = "data-dir"
  lazy val sleepTime = "sleep-time"
  lazy val amqServer = "amq-server"
  lazy val amqQueue = "amq-queue"

}

object BalboaAgent extends App with Config with BalboaLogging {

  override def main(args: Array[String]): Unit = {

    logger info "Loading Balboa Agent Configuration!"

    var dataDir = dataDirectory(null)
    var st = sleepTime(MetricQueue.AGGREGATE_GRANULARITY)
    var amqServer = activemqServer
    var amqQueue = activemqQueue

    val optParser: OptionParser = new OptionParser()
    // Can use a single configuration file for all command line application.
    val fileOpt = optParser.accepts(CLIParamKeys.dataDir, "Directory that contains Metrics Data.")
      .withRequiredArg()
      .ofType(classOf[File])
    val sleepOpt = optParser.accepts(CLIParamKeys.sleepTime, "Scheduled amount of time (ms) that the service will sleep before restarting.")
      .withRequiredArg()
      .ofType(classOf[Long])
    val amqServerOpt = optParser.accepts(CLIParamKeys.amqServer, "Active MQ Server to connect to.")
      .withRequiredArg()
      .ofType(classOf[String])
    val amqQueueOpt = optParser.accepts(CLIParamKeys.amqQueue, "Active MQ Queue to publish to.")
      .withRequiredArg()
      .ofType(classOf[String])

    // Overwrite properties with any Command Line Arguments.
    val set: OptionSet = optParser.parse(args: _*)
    set.valueOf(fileOpt) match {
      case d: File =>
        logger info s"Overwriting directory to ${d.getAbsolutePath}"
        dataDir = d
      case _ => // NOOP
    }
    if (set.has(sleepOpt)) {
      set.valueOf(sleepOpt) match {
        case time: Long =>
          logger info s"Overwriting sleep time to $time"
          st = time
      }
    }
    set.valueOf(amqServerOpt) match {
      case s: String =>
        logger info s"Overwriting AMQ Server to $s"
        amqServer = s
      case _ => // NOOP
    }
    set.valueOf(amqQueueOpt) match {
      case s: String =>
        logger info s"Overwriting AMQ Queue to $s"
        amqQueue = s
      case _ => // NOOP
    }

    logger info "Starting Balboa Agent"
    new MetricConsumer(dataDir, st, amqServer, amqQueue).run()
  }


}
