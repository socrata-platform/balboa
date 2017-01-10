package com.socrata.balboa.agent

import java.io._
import java.lang.{Double => JavaDouble, Long => JavaLong}
import java.util
import java.util.regex.Pattern

import com.codahale.metrics.Timer
import com.socrata.balboa.agent.metrics.BalboaAgentMetrics
import com.socrata.balboa.metrics.Metric
import com.socrata.balboa.util.FileUtils
import com.socrata.metrics.{Fluff, MetricQueue}
import com.typesafe.scalalogging.LazyLogging

import scala.util.control.Breaks.{break, breakable}
import scala.util.{Failure, Success, Try}

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

object MetricConsumer {
  val TIMESTAMP: String = "timestamp"
  val ENTITY_ID: String = "entityId"
  val NAME: String = "name"
  val VALUE: String = "value"
  val RECORD_TYPE: String = "type"
  val fields: List[String] = List(TIMESTAMP, ENTITY_ID, NAME, VALUE, RECORD_TYPE)
  val integerPattern: Pattern = "-?[0-9]+".r.pattern
}

/**
  * Creates a MetricConsumer that processes files from `directory` and emits them to `metricPublisher`.  The `fileProvider`
  * is a control mechanism that allows clients to make determinations on which [[java.io.File]]s can be processed.
  *
  * @param directory       Directory in which to process metrics.
  * @param metricPublisher Queue to emit metrics to.
  * @param fileProvider    The { @link FileFilter} used to determine which files are allowed to be processed.
  */
class MetricConsumer(val directory: File, val metricPublisher: MetricQueue, val fileProvider: MetricFileProvider)
  extends Runnable with AutoCloseable with LazyLogging {

  if (directory == null || !directory.isDirectory) throw new IllegalArgumentException("Illegal Data directory " + directory)
  if (metricPublisher == null) throw new NullPointerException("Metric Queue cannot be null")

  /**
    * Creates Metric consumer that will attempt to find all the metric data within a directory
    * and push them to a metricPublisher. Does not take responsibility for closing the metricPublisher.
    */
  def this(directory: File, metricPublisher: MetricQueue) {
    this(directory, metricPublisher, AlphabeticMetricFileProvider(directory.toPath))
  }

  /**
    * Attempts to process all the sub directories on the root directories for all possible
    * metrics.
    */
  def run() {
    logger.info("Looking for metrics files recursively in '{}'", this.directory.getAbsolutePath)
    val runTimer: Timer.Context = BalboaAgentMetrics.totalRuntime.time
    val start: Long = System.currentTimeMillis
    var recordsProcessed: Int = 0
    // Treat each individual Metric data file as its own isolated run.
    // We are trying to prevent the failure to process one file from blocking or preventing the processing
    // of others.
    val files: Set[File] = fileProvider.provide
    files.foreach { metricsEventLog: File =>
      logger.info(s"Processing '${metricsEventLog.getAbsolutePath}'.")
      val maybeRecords: Try[List[MetricsRecord]] = Try(processFile(metricsEventLog))
      maybeRecords match {
        case Failure(e: IOException) =>
          logger.error(s"Error reading records from $metricsEventLog", e)
          BalboaAgentMetrics.metricsProcessingFailureCounter.inc()
          val broken: File = new File(metricsEventLog.getAbsolutePath + FileUtils.BROKEN_FILE_EXTENSION)
          if (!metricsEventLog.renameTo(broken)) {
            logger.warn(s"Unable to rename broken file $metricsEventLog permissions issue?")
            BalboaAgentMetrics.renameBrokenFileFailureCounter.inc()
          }
        case Success(records) =>
          records.foreach { r =>
            metricPublisher.create(Fluff(r.entityId), Fluff(r.name), r.value.longValue, r.timestamp, r.metricType)
          }
          recordsProcessed += records.size
          if (!metricsEventLog.delete) {
            logger.error(s"Unable to delete event log $metricsEventLog - file may be read twice, which is bad.")
            BalboaAgentMetrics.deleteEventFailureCounter.inc()
          }
        case _ =>
      }
    }
    val processingTime: Long = System.currentTimeMillis - start
    logger.info(s"Run completed, processed $recordsProcessed in $processingTime ms")
    BalboaAgentMetrics.metricsEmittedCount.inc(recordsProcessed)
    BalboaAgentMetrics.metricsEmittedMeter.mark(recordsProcessed)
    runTimer.stop
  }

  /**
    * This method does nothing. As the MetricConsumer does not take unique
    * ownership of its metricPublisher, it cannot be sure that it is safe to close.
    */
  @throws[Exception]
  def close() {
  }

  /**
    * Given a metrics data file, attempt to extract all the metrics from the file and
    * pushes these metrics into the underlying metricPublisher.
    *
    * @param f File to process.
    * @return A list of { @link MetricsRecord}s that were process.
    * @throws IOException When there is a problem processing the file.
    */
  @throws[IOException]
  private def processFile(f: File): List[MetricsRecord] = {
    val filePath: String = f.getAbsolutePath
    logger.info("Processing file {}", filePath)
    var results: Vector[MetricsRecord] = Vector.empty
    val stream: InputStream = new BufferedInputStream(new FileInputStream(f))
    try {
      var record: util.Map[String, String] = readRecord(stream)
      while (record != null) {
        breakable {
          val rawValue: String = record.get(MetricConsumer.VALUE)
          var value: Number = null
          if (!rawValue.equalsIgnoreCase("null")) {
            try {
              if (MetricConsumer.integerPattern.matcher(rawValue).matches) {
                value = JavaLong.valueOf(rawValue)
              } else {
                value = JavaDouble.valueOf(rawValue)
              }
            }
            catch {
              case e: NumberFormatException =>
                logger.error(s"NumberFormatException reading metric from record: ${record.toString}", e)
                BalboaAgentMetrics.errorInvalidValueCounter.inc()
                break()
            }
          }
          val entityId = record.get(MetricConsumer.ENTITY_ID)
          val name = record.get(MetricConsumer.NAME)
          val timestamp = JavaLong.parseLong(record.get(MetricConsumer.TIMESTAMP))
          val metricType = Metric.RecordType.valueOf(record.get(MetricConsumer.RECORD_TYPE).toUpperCase)
          results = results :+ new MetricsRecord(entityId, name, value, timestamp, metricType)
        }
        record = readRecord(stream)
      }
    } finally {
      logger.info("Completed (possibly with errors) file {}", filePath)
      // Percolate the exception up the call stack.... ugh.
      stream.close()
    }
    results.toList
  }

  /**
    * Accepts a stream of bytes and deserialize them into a single Metric.
    *
    * <b>
    * This class works under the assumptions that bytes are serialized under the following format
    * <ul>
    * <li>0xff - single byte - Beginning mark of a single metrics entry</li>
    * <li>Sequence of bytes of undetermined length - A utf-8 byte String encoded to bytes of a timestamp of type long.</li>
    * <li>0xfe - single byte - end of timestamp sequence</li>
    * <li>Sequence of bytes of undetermined length - A utf-8 byte String encoded to bytes of the entity id</li>
    * <li>0xfe - single byte - end of entity id byte sequence</li>
    * <li>Sequence of bytes of undetermined length - A utf-8 byte String encoded to bytes of the metric name</li>
    * <li>0xfe - single byte - end of metric name byte sequence</li>
    * <li>Sequence of bytes of undetermined length - A utf-8 byte String encoded to bytes of the metric value. The metric value is of type Number</li>
    * <li>0xfe - single byte - end of metric value byte sequence</li>
    * <li>Sequence of bytes of undetermined length - A utf-8 byte String encoded to bytes of the metric type. See [[com.socrata.balboa.metrics.Metric.RecordType]]</li>
    * <li>0xfe - single byte - end of metric type byte sequence</li>
    * </ul>
    * </b>
    *
    * @param stream A stream of bytes that represent a Single Metric.
    * @return A Map representing a single Metric entry. null if end of file has been reached.
    * @throws IOException If an incomplete metrics record was identified.
    */
  @throws[IOException]
  private def readRecord(stream: InputStream): util.Map[String, String] = {
    // First we have to find the start-of-record (a 0xff byte).
    // It *should* be the very first byte we're looking at.
    if (!seekToHeadOfMetrics(stream)) return null
    val record: util.Map[String, String] = new util.HashMap[String, String]
    for (field <- MetricConsumer.fields) {
      val fieldValue: String = readField(stream)
      if (fieldValue == null) {
        // This was the last record was processed with the prior iteration.
        return null
      }
      record.put(field, fieldValue)
    }
    record
  }

  /**
    * Given a stream of bytes that does not begin with 0xff, read the bytes until 0xfe is reached.  The
    * resulting bytes read will be interpreted as a utf-8 String representation of the field value.
    *
    * @param stream Stream of bytes that represent a field value.
    * @return utf-8 String representation of the bytes read.
    * @throws IOException An error occurred while processing the Stream.
    */
  @throws[IOException]
  private def readField(stream: InputStream): String = {
    val baos: ByteArrayOutputStream = new ByteArrayOutputStream

    var b: Int = stream.read

    while (b != 0xfe) {
      b match {
        case -1 => // EOF
          val baosSize: Int = baos.size
          if (baosSize > 0)
            logger.warn(
              s"Reached end of file with $baosSize bytes not processed. " +
              s"This could mean lost or corrupted metrics data")
          return null
        case 0xff =>
          logger.warn("Found an incomplete record.")
          BalboaAgentMetrics.incompleteFieldCounter.inc()
          throw new IOException(
            "Unexpected 0xFF field in file. Refusing to continue " +
            "to process since our file is almost certainly corrupt.")
        case _ => // Expect that all other bytes are apart of the field value.
          baos.write(b)
      }
      b = stream.read
    }
    // We have successfully found the end of the metric field.
    new String(baos.toByteArray, "utf-8")
  }

  /**
    * Given a stream of bytes that represent a stream of serialized metrics entries, this method
    * reads bytes off of the stream until it reads the 0xff or EOF.  Once the head of the metrics
    * sequence is read the stream will be set to read the next byte.
    *
    * <br>
    * NOTE: This method does alter the state of the input stream.
    * </br>
    *
    * @param stream The InputStream of bytes.
    * @return True whether the Stream is ready to read. False if the Stream does not provide any metrics.
    * @throws IOException There is a problem reading from the stream.
    */
  @throws[IOException]
  private def seekToHeadOfMetrics(stream: InputStream): Boolean = {
    var b: Int = stream.read
    while (b != -1) {
      b match {
        case 0xff =>
          return true
        case _ =>
          logger.warn(s"Unexpected byte: $b found.  Continuing to seek until head of metrics.")
      }
      b = stream.read
    }
    // Reached EOF
    false
  }

  override def toString: String = s"MetricConsumer{directory=$directory, metricPublisher=$metricPublisher}"
}
