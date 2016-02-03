package com.socrata.balboa.agent

import java.nio.file.{Path, Files}

import com.blist.metrics.impl.queue.MetricFileQueue
import com.socrata.balboa.metrics.Metric
import com.socrata.metrics.{MetricIdParts, Fluff, MetricQueue}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.mockito.Matchers
import org.mockito.Mockito._
import org.scalatest.WordSpec
import org.scalatest.ShouldMatchers
import org.scalatest.mock.MockitoSugar

/**
  * Unit Tests for [[MetricConsumer]]
  */
class MetricConsumerSpec extends WordSpec with ShouldMatchers with MockitoSugar with StrictLogging {

  trait TestMetrics {
    private val time = System.currentTimeMillis()
    val testMetrics = (1 to 100) map (i =>
      new MetricsRecord(s"entity_id_$i", s"metric_$i", i, time + i, Metric.RecordType.AGGREGATE)
      )
  }

  trait MockQueue {
    val mockQueue = mock[MetricQueue]
  }

  trait OneRootDirectory extends MockQueue {
    val rootDataPath: Path = Files.createTempDirectory("metrics-")
    val metricConsumer = new MetricConsumer(rootDataPath.toFile, mockQueue)
    val fileQueues = (1 to 5) map (_ => new MetricFileQueue(rootDataPath.toFile))
  }


  private def writeToQueue(queue: MetricQueue, records: MetricsRecord*) = {
    records.foreach(r => queue.create(
      MetricIdParts(r.getEntityId), Fluff(r.getName), r.getValue.longValue(), r.getTimestamp, r.getType)
    )
  }

  "A Metric Consumer" when {

    "there is a single metrics root directory" when {

      "no metrics data exists" should {

        "emit no metrics" in new OneRootDirectory {
          verify(mockQueue, never()).create(Matchers.any[MetricIdParts](), Matchers.any[MetricIdParts](), Matchers.anyLong(),
            Matchers.anyLong(), Matchers.any[Metric.RecordType]())
        }

      }

      "1 metrics data file exists" should {

        "not read" in new OneRootDirectory {
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
      "more then 1 metrics data files exists" should {

        "" in new OneRootDirectory with TestMetrics {
          logger.info(s"Writing ${testMetrics.head} using ${fileQueues.size} file queues")
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

    }



    //    "emits multiple metrics when multiple metrics were written to disk " in new MetricWriters {
    //
    //    }
  }

}
