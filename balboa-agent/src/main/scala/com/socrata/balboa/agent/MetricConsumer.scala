package com.socrata.balboa.agent

import java.io._
import java.util.regex.Pattern

import com.codahale.metrics.Timer
import com.socrata.balboa.agent.metrics.BalboaAgentMetrics
import com.socrata.balboa.util.FileUtils
import com.socrata.metrics.{Fluff, MetricQueue}
import com.typesafe.scalalogging.LazyLogging
import resource._
import scodec.Attempt.{Successful, Failure => AttemptFailure}
import scodec.bits.BitVector
import scodec.{Codec, DecodeResult}

/**
  * The MetricConsumer consumes metrics from data files from within a specific directory.  Any metrics extracted will
  * be pushed to a specified [[com.socrata.metrics.MetricQueue]].
  *
  * <br>
  *
  * Given a root directory, the MetricConsumer will recursively extract all the directories under the root directory.
  * For each directory, files are processed in reverse alphabetical order excluding the last file in the order.  It is
  * common and suggested that metric producers use timestamps in their file names and not place any additional files
  * within the same directory they write metrics to.
  *
  */
//  TODO: Make this contract better (assuming last file off-limits is dumb)

// scalastyle:off field.name
object MetricConsumer {
  val TIMESTAMP: String = "timestamp"
  val ENTITY_ID: String = "entityId"
  val NAME: String = "name"
  val VALUE: String = "value"
  val RECORD_TYPE: String = "type"
  val fields: List[String] = List(TIMESTAMP, ENTITY_ID, NAME, VALUE, RECORD_TYPE)
  val integerPattern: Pattern = "-?[0-9]+".r.pattern
}
// scalastyle:on field.name

/**
  * Creates a MetricConsumer that processes files from `directory` and emits them to `metricPublisher`.  The
  * `fileProvider` is a control mechanism that allows clients to make determinations on which [[java.io.File]]s can
  * be processed.
  *
  * @param directory       Directory in which to process metrics.
  * @param metricPublisher Queue to emit metrics to.
  * @param fileProvider    The { @link FileFilter} used to determine which files are allowed to be processed.
  */
class MetricConsumer(val directory: File, val metricPublisher: MetricQueue, val fileProvider: MetricFileProvider)
  extends Runnable with AutoCloseable with LazyLogging {
  require(Option(directory).nonEmpty, "Directory cannot be null")
  require(directory.isDirectory, s"$directory is not a directory")
  require(Option(metricPublisher).nonEmpty, "Metric Queue cannot be null")

  /**
    * Creates Metric consumer that will attempt to find all the metric data within a directory
    * and push them to a metricPublisher. Does not take responsibility for closing the metricPublisher.
    */
  def this(directory: File, metricPublisher: MetricQueue) { // scalastyle:ignore
    this(directory, metricPublisher, AlphabeticMetricFileProvider(directory.toPath))
  }

  /**
    * Attempts to process all the sub directories on the root directories for all possible
    * metrics.
    */
  def run(): Unit = {
    logger.info(s"Looking for metrics files recursively in '${directory.getAbsoluteFile}'")
    val runTimer: Timer.Context = BalboaAgentMetrics.totalRuntime.time
    val start: Long = System.currentTimeMillis
    // Treat each individual Metric data file as its own isolated run.
    // We are trying to prevent the failure to process one file from blocking or preventing the processing
    // of others.
    // the "Alphabetic" file provider returns a set, which is not guaranteed to be in order.  Sorting here to avoid
    // a larger-than-necessary refactor... for now.
    val files: List[File] = fileProvider.provide.toList.sortBy(_.getName)
    val recordsProcessed: Int = files.foldLeft(0) { (count: Int, metricsEventLog: File) =>
      logger.info(s"Processing '${metricsEventLog.getAbsolutePath}'.")
      val recordsOrErrors = processFile(metricsEventLog)
      val newRecordsCount = recordsOrErrors match {
        case Left(errors) =>
          errors.foreach(logger.error(s"Error reading records from $metricsEventLog", _))
          BalboaAgentMetrics.metricsProcessingFailureCounter.inc()
          val broken: File = new File(metricsEventLog.getAbsolutePath + FileUtils.BROKEN_FILE_EXTENSION)
          if (!metricsEventLog.renameTo(broken)) {
            logger.warn(s"Unable to rename broken file $metricsEventLog permissions issue?")
            BalboaAgentMetrics.renameBrokenFileFailureCounter.inc()
          }
          0
        case Right(records) =>
          records.foreach(publishRecord)
          if (!metricsEventLog.delete) {
            logger.error(s"Unable to delete event log $metricsEventLog - file may be read twice, which is bad.")
            BalboaAgentMetrics.deleteEventFailureCounter.inc()
          }
          records.size
      }
      newRecordsCount + count
    }
    val processingTime: Long = System.currentTimeMillis - start
    logger.info(s"Run completed, processed $recordsProcessed in $processingTime ms")
    BalboaAgentMetrics.metricsEmittedCount.inc(recordsProcessed)
    BalboaAgentMetrics.metricsEmittedMeter.mark(recordsProcessed)
    runTimer.stop
  }

  private def publishRecord(r: MetricsRecord): Unit = {
    metricPublisher.create(Fluff(r.entityId), Fluff(r.name), r.value.longValue, r.timestamp, r.metricType)
  }

  /**
    * This method does nothing. As the MetricConsumer does not take unique
    * ownership of its metricPublisher, it cannot be sure that it is safe to close.
    */
  def close(): Unit = {
  }

  /**
    * Given a metrics data file, attempt to extract all the metrics from the file and
    * pushes these metrics into the underlying metricPublisher.
    *
    * @param f File to process.
    * @return Either a list of { @link MetricsRecord}s that were process,
    *         or list of unexpected errors occurred during processing
    */
  private def processFile(f: File): Either[Seq[Throwable], List[MetricsRecord]] = {
    val filePath: String = f.getAbsolutePath
    logger.info(s"Processing file $filePath")
    val managedFileInputStream = managed(new FileInputStream(f))

    managedFileInputStream.map { fileInputStream =>
      val bitVector = BitVector.fromMmap(fileInputStream.getChannel)
      Codec.decodeCollect[List, MetricsRecord](MetricsRecord.codec.asDecoder, None)(bitVector)
    }.either.right.flatMap {
      case Successful(DecodeResult(value, remainder)) =>
        if (remainder.nonEmpty) {
          Left(
            Seq(new Error(s"Metric records file $filePath had remaining bits after decoding; is probably incomplete"))
          )
        } else {
          Right(value)
        }
      case AttemptFailure(cause) =>
        Left(Seq(new Error(s"Error decoding metric records: ${cause.messageWithContext}")))
    }
  }

  override def toString: String = s"MetricConsumer{directory=$directory, metricPublisher=$metricPublisher}"
}
