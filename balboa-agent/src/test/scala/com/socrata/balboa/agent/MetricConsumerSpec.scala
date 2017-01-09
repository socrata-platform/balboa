package com.socrata.balboa.agent

import java.nio.file.{Files, Path}

import com.blist.metrics.impl.queue.MetricFileQueue
import com.socrata.balboa.metrics.Metric
import com.socrata.metrics.{Fluff, MetricIdParts, MetricQueue}
import com.typesafe.scalalogging.StrictLogging
import org.mockito.Matchers
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ShouldMatchers, WordSpec}

import scala.collection.immutable.IndexedSeq

/**
  * Unit Tests for [[MetricConsumer]]
  */
class MetricConsumerSpec extends WordSpec with ShouldMatchers with MockitoSugar with StrictLogging {

  import org.mockito.Mockito._

  /**
    * The MetricFileQueue is designed to write to the same file given if subsequent writes occur at the same time.
    * This makes testing nondeterministic right at the same System.currentTime().
    */
  private val queueCreateMetricWaitTimeMS = 5

  /**
    * Number of internal metrics directories.
    */
  private val numMultipleMetricsDir = 5

  trait TestMetrics {
    private val time = System.currentTimeMillis()
    val testMetrics: IndexedSeq[MetricsRecord] = (1 to 10) map (i =>
      new MetricsRecord(s"entity_id_$i", s"metric_$i", i, time + i, Metric.RecordType.AGGREGATE)
      )
  }

  trait MockQueue {
    val mockQueue: MetricQueue = mock[MetricQueue]
  }

  /**
    * Represents a single directory serving as the metrics root data file.
    */
  trait OneRootDirectory extends MockQueue {
    val rootMetricsDir: Path = Files.createTempDirectory("metric-consumer-spec-")
    val metricConsumer = new MetricConsumer(rootMetricsDir.toFile, mockQueue)

    // a mapping of metrics directories and file queues that write to that directory.
    val numFileQueues = 5
    val fileQueues: Map[Path, Seq[MetricFileQueue]] =
      Map(rootMetricsDir -> createFileQueues(rootMetricsDir, numFileQueues))

    /**
      * Creates `num` individual file queues for a single directory.
      *
      * @param num Number of queues to create
      * @return Sequence of MetricFileQueues
      */
    def createFileQueues(dir: Path, num: Int): Seq[MetricFileQueue] =
      (1 to num) map (_ => new MetricFileQueue(dir.toFile))
  }

  trait MultipleRootDirectories extends OneRootDirectory {
    val metricDirs: IndexedSeq[Path] = (1 to numMultipleMetricsDir).map(i => {
      val dir = rootMetricsDir.resolve(s"metrics-dir-$i")
      Files.createDirectory(dir)
      dir
    })
    override val fileQueues: Map[Path, Seq[MetricFileQueue]] =
      metricDirs.map(d => (d, Seq(new MetricFileQueue(d.toFile)))).toMap
  }

  /**
    * Uses `queue` to write a variable collection of MetricsRecords.
    *
    * @param queue The MetricQueue used to emit metrics.
    * @param records MetricsRecords to emit to the metric queue.
    */
  private def writeToQueue(queue: MetricQueue, records: MetricsRecord*)() = {
    records.foreach(r => {
      queue.create(Fluff(r.entityId), Fluff(r.name), r.value.longValue(), r.timestamp, r.metricType)
      Thread.sleep(queueCreateMetricWaitTimeMS)
    })
  }

  /**
    * Test a Metric Consumer should never send any messages.
    *
    * @param metricConsumer The MetricConsumer that reads files from disk and sends to Metric Queue.
    * @param mockQueue The internal mock queue that should not be receiving messages.
    */
  private def testNeverEmitMetrics(metricConsumer: MetricConsumer, mockQueue: MetricQueue): Unit = {
    metricConsumer.run()
    metricConsumer.close()
    verify(mockQueue, never()).create(Matchers.any[MetricIdParts](),
      Matchers.any[MetricIdParts](),
      Matchers.anyLong(),
      Matchers.anyLong(),
      Matchers.any[Metric.RecordType]())
  }

  /**
    * When the metric consumer should consume from disk and emits to JMS, it does.  For every MetricFileQueue
    * in `fileQueues` emit every metric in `metrics`.  This function also verifies that every metric in `metrics` is
    * created with `mockQueue` `numTimesMetricEmitted` times.
    *
    * @param metricConsumer The Metric Consumer instance to test
    * @param mockQueue The mocked MetricQueue that the metric consumer uses to emit metric messages.
    * @param metrics The metrics to consume and emit.
    * @param fileQueues The MetricFileQueues used to send each metric.
    * @param numTimesMetricEmitted The number of times a single metric in `metrics` was emitted.
    */
  private def testEmitsMetrics(metricConsumer: MetricConsumer,
                               mockQueue: MetricQueue,
                               metrics: Seq[MetricsRecord],
                               fileQueues: Seq[MetricFileQueue],
                               numTimesMetricEmitted: Int): Unit = {
    logger.info(s"Writing ${metrics.size} metrics with ${fileQueues.size} different file queues")
    fileQueues.foreach(q => writeToQueue(q, metrics: _*))
    metricConsumer.run()
    metricConsumer.close()
    // For every metric written to disk, all but the metrics in youngest file should be processed.
    metrics.foreach(m => {
      verify(mockQueue, times(numTimesMetricEmitted)).create(
        Fluff(m.entityId),
        Fluff(m.name),
        m.value.longValue(),
        m.timestamp,
        m.metricType
      )
    })
  }

  "A Metric Consumer" when {
    "the root directory does not exists" should {
      "throw an IllegalArgumentException" in new MockQueue {
        intercept[IllegalArgumentException] {
          new MetricConsumer(Files.createTempDirectory("metrics").resolve("nonexistent_subdirectory").toFile, mockQueue)
        }
      }
    }
    "the root file is not a directory" should {
      "throw an IllegalArgumentException" in new MockQueue {
        intercept[IllegalArgumentException] {
          new MetricConsumer(Files.createTempFile("metric", ".txt").toFile, mockQueue)
        }
      }
    }
    "there is a single root directory" when {
      "there is no metrics data" should {
        "emit no metrics" in new OneRootDirectory {
          verify(mockQueue, never()).create(Matchers.any[MetricIdParts](), Matchers.any[MetricIdParts](),
            Matchers.anyLong(), Matchers.anyLong(), Matchers.any[Metric.RecordType]())
        }
      }
      "there is 1 metrics data file" should {
        "not process any files" in new OneRootDirectory {
          testNeverEmitMetrics(metricConsumer, mockQueue)
        }
      }
      // Metric Consumer does not actually read any metrics if a single file exists.
      "there are more then 1 metrics data file with a single metric" should {
        "process 1 metric from all but the youngest file" in new OneRootDirectory with TestMetrics {
          testEmitsMetrics(metricConsumer, mockQueue, Seq(testMetrics.head), fileQueues(rootMetricsDir),
            fileQueues(rootMetricsDir).size - 1)
        }
      }
      "there are more then 1 metrics data file with multiple metrics" should {
        "process all metrics from all but the youngest file." in new OneRootDirectory with TestMetrics {
          testEmitsMetrics(metricConsumer, mockQueue, testMetrics, fileQueues(rootMetricsDir),
            fileQueues(rootMetricsDir).size - 1)
        }
      }
    }
    "there are multiple subdirectories" when {
      "there are no metrics data files" should {
        "not process any files" in new MultipleRootDirectories {
          testNeverEmitMetrics(metricConsumer, mockQueue)
        }
      }
      "there is one metric data file in each directory" should {
        "not process any files" in new MultipleRootDirectories with TestMetrics {
          testNeverEmitMetrics(metricConsumer, mockQueue)
        }
      }
      "there are multiple metrics data files in one directory" should {
        "process a single directory" in new MultipleRootDirectories with TestMetrics {
          val fullDirPath: Path = fileQueues.head._1
          // We are creating a 5 queues for a given metric directory
          // 5 individual file queues correspond to 5 individual metric data files for any given metric directory.
          val numMetricFiles = 5
          val multipleQueues: Seq[MetricFileQueue] =
            fileQueues(fullDirPath) ++ createFileQueues(fullDirPath, numMetricFiles)
          val newFileQueues: Map[Path, Seq[MetricFileQueue]] = fileQueues + (fullDirPath -> multipleQueues)
          testEmitsMetrics(
            metricConsumer,
            mockQueue,
            testMetrics,
            newFileQueues.flatMap { case (_, fileQs) => fileQs }.toSeq,
            multipleQueues.size - 1
          )
        }
      }
      "there are multiple metrics data files in multiple directories" should {
        "process all directories" in new MultipleRootDirectories with TestMetrics {
          // We are creating a 5 queues for a given metric directory
          // 5 individual file queues correspond to 5 individual metric data files for any given metric directory.
          val numMetricFiles = 5
          val newFileQueues: Map[Path, Seq[MetricFileQueue]] = fileQueues.map { case (path, _) =>
              path -> createFileQueues(path, numMetricFiles)
            }
          testEmitsMetrics(
            metricConsumer,
            mockQueue,
            testMetrics,
            newFileQueues.flatMap { case (_, fileQs) => fileQs }.toSeq,
            newFileQueues.foldLeft(0) { case (totalFqs, (_, fileQs)) => totalFqs + fileQs.size } - newFileQueues.size
          )
        }
      }
    }
  }
}
