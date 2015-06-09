package com.socrata.balboa.tickertape

import java.io.File

import com.blist.metrics.impl.queue.MetricFileQueue
import com.socrata.balboa.common.logging.BalboaLogging
import com.socrata.metrics.{Fluff, MetricIdPart}
import joptsimple.{OptionParser, OptionSet}

object CLIParamKeys {

  lazy val dataDir = "data-dir"
  lazy val sleepTime = "sleep-time"
  lazy val batchSize = "batch-size"

}

object BalboaTickerTape extends App with Config with BalboaLogging {

  override def main(args: Array[String]): Unit = {

    logger info "Loading Balboa Agent Configuration!"

    var dataDir = dataDirectory(null)
    var st = sleepTime()
    var bs = batchSize()

    val optParser: OptionParser = new OptionParser()
    // Can use a single configuration file for all command line application.
    val fileOpt = optParser.accepts(CLIParamKeys.dataDir, "Directory that contains Metrics Data.")
      .withRequiredArg()
      .ofType(classOf[File])
    val sleepOpt = optParser.accepts(CLIParamKeys.sleepTime, "Scheduled amount of time (ms) that the service will sleep before restarting.")
      .withRequiredArg()
      .ofType(classOf[Long])
    val batchSizeOpt = optParser.accepts(CLIParamKeys.batchSize, "Number of metrics to write on each batch.")
      .withRequiredArg()
      .ofType(classOf[Int])

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
        case time: Long if time > 0 =>
          logger info s"Overwriting sleep time to $time"
          st = time
      }
    }
    if (set.has(batchSizeOpt)) {
      set.valueOf(batchSizeOpt) match {
        case i: Int =>
          logger info s"Overwriting batch size to $i"
          bs = i
      }
    }

    logger info s"Configured to write to ${dataDir.getAbsolutePath}."
    logger info s"Configured to sleep $st (ms) between batch writes."
    logger info s"Configured to emit $bs for each batch."
    logger info "Starting Balboa Stupid Service"

    val idPart = new MetricIdPart("metrics-internal-test")
    while (true) {
      logger info s"Writing $bs metrics"
      0 until bs foreach { i =>
        val q = MetricFileQueue.getInstance(dataDir)
        q.create(idPart, Fluff(s"fake-metric-$i"), 1)
      }
      Thread.sleep(st)
    }
  }

}
