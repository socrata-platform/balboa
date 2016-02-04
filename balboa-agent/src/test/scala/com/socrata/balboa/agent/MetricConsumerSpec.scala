package com.socrata.balboa.agent

import java.nio.file.{Files, Path}

import com.blist.metrics.impl.queue.MetricFileQueue
import com.socrata.balboa.metrics.Metric
import com.socrata.metrics.{Fluff, MetricIdParts, MetricQueue}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.mockito.Matchers
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ShouldMatchers, WordSpec}

/**
  * Unit Tests for [[MetricConsumer]]
  */
class MetricConsumerSpec extends WordSpec with ShouldMatchers with MockitoSugar with StrictLogging {

  /**
    * The MetricFileQueue is designed to write to the same file given if subsequent writes occur at the same time.
    * This makes testing indetermistic right at the same System.currentTime().
    */
  private val FILE_WRITE_WAIT_TIME_MS = 5

  /**
    * Number of internal metrics directories.
    */
  private val NUM_MULTIPLE_METRICS_DIR = 5

  trait TestMetrics {
    private val time = System.currentTimeMillis()
    val testMetrics = (1 to 10) map (i =>
      new MetricsRecord(s"entity_id_$i", s"metric_$i", i, time + i, Metric.RecordType.AGGREGATE)
      )
  }

  trait MockQueue {
    val mockQueue = mock[MetricQueue]
  }

  /**
    * Represents a single directory serving as the metrics root data file.
    */
  trait OneRootDirectory extends MockQueue {
    val rootMetricsDir: Path = Files.createTempDirectory("metric-consumer-spec-")
    val metricConsumer = new MetricConsumer(rootMetricsDir.toFile, mockQueue)
    val fileQueues = (1 to 5) map (_ => new MetricFileQueue(rootMetricsDir.toFile))
  }

  trait MultipleRootDirectories extends OneRootDirectory {
    val metricDirs = (1 to NUM_MULTIPLE_METRICS_DIR).map(i => {
      val dir = rootMetricsDir.resolve(s"metrics-dir-$i")
      Files.createDirectory(dir)
      dir.toFile
    })
    override val fileQueues = metricDirs.map(d => new MetricFileQueue(d))
  }

  private def writeToQueue(queue: MetricQueue, records: MetricsRecord*) = {
    records.foreach(r => {
      queue.create(
        Fluff(r.getEntityId), Fluff(r.getName), r.getValue.longValue(), r.getTimestamp, r.getType)
      Thread.sleep(FILE_WRITE_WAIT_TIME_MS)
    })
  }

  "A Metric Consumer" when {

    "the root directory does not exists" should {

      "an IllegalArgumentException is thrown" in new MockQueue {
        intercept[IllegalArgumentException] {
          new MetricConsumer(Files.createTempDirectory("metrics").resolve("nonexistent_subdirectory").toFile, mockQueue)
        }
      }
    }

    "the root file is not a directory" in new MockQueue {
      intercept[IllegalArgumentException] {
        new MetricConsumer(Files.createTempFile("metric", ".txt").toFile, mockQueue)
      }
    }

    "there is a single root directory" when {

      "there is no metrics data exists" should {

        "emit no metrics" in new OneRootDirectory {
          verify(mockQueue, never()).create(Matchers.any[MetricIdParts](), Matchers.any[MetricIdParts](), Matchers.anyLong(),
            Matchers.anyLong(), Matchers.any[Metric.RecordType]())
        }

      }

      "there is 1 metrics data file exists" should {

        "not process any files" in new OneRootDirectory {
          metricConsumer.run()
          metricConsumer.close()
          verify(mockQueue, never()).create(Matchers.any[MetricIdParts](),
            Matchers.any[MetricIdParts](),
            Matchers.anyLong(),
            Matchers.anyLong(),
            Matchers.any[Metric.RecordType]())
        }

      }

      // Metric Consumer does not actually read any metrics if a single file exists.
      "there exists more then 1 metrics data file with a single metric" should {

        "process 1 metric from all but the youngest file" in new OneRootDirectory with TestMetrics {
          logger.info(s"Writing ${testMetrics.head} with ${fileQueues.size} file queues")
          fileQueues.foreach(q => writeToQueue(q, testMetrics.head))
          metricConsumer.run()
          metricConsumer.close()
          verify(mockQueue, times(fileQueues.size - 1)).create(
            Fluff(testMetrics.head.getEntityId),
            Fluff(testMetrics.head.getName),
            testMetrics.head.getValue.longValue(),
            testMetrics.head.getTimestamp,
            testMetrics.head.getType
          )
        }
      }

      "there exists more then 1 metrics data file with multiple metrics" should {

        "process all metrics from all but the youngest file." in new OneRootDirectory with TestMetrics {
          logger.info(s"Writing ${testMetrics.size} with ${fileQueues.size} file queues")
          fileQueues.foreach(q => writeToQueue(q, testMetrics: _*))
          metricConsumer.run()
          metricConsumer.close()
          testMetrics.foreach(m => {
            verify(mockQueue, times(fileQueues.size - 1)).create(
              Fluff(m.getEntityId),
              Fluff(m.getName),
              m.getValue.longValue(),
              m.getTimestamp,
              m.getType
            )
          })
        }
      }
    }

    "there are multiple subdirectories" when {

      "there are no metrics data files" should {
        "not process any files" in new MultipleRootDirectories {
          metricConsumer.run()
          metricConsumer.close()
          verify(mockQueue, never()).create(Matchers.any[MetricIdParts](),
            Matchers.any[MetricIdParts](),
            Matchers.anyLong(),
            Matchers.anyLong(),
            Matchers.any[Metric.RecordType]())
        }
      }

      "there is one metric data file in each directory" should {

        "not process any files" in new MultipleRootDirectories {
          metricConsumer.run()
          metricConsumer.close()
          verify(mockQueue, never()).create(Matchers.any[MetricIdParts](),
            Matchers.any[MetricIdParts](),
            Matchers.anyLong(),
            Matchers.anyLong(),
            Matchers.any[Metric.RecordType]())
        }
      }

      "there are multiple metrics data files in one directory" should {

        "process the single directory" in new MultipleRootDirectories {

        }

      }

    }
  }
}
