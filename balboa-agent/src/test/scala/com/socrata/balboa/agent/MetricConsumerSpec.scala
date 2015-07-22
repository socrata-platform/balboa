package com.socrata.balboa.agent

import java.nio.file.{Path, Files}

import com.blist.metrics.impl.queue.MetricFileQueue
import com.socrata.balboa.metrics.Metric
import com.socrata.metrics.{MetricIdParts, MetricQueue}
import org.mockito.Matchers
import org.mockito.Mockito._
import org.scalatest.WordSpec
import org.scalatest.ShouldMatchers
import org.scalatest.mock.MockitoSugar

/**
 * Unit Tests for [[MetricConsumer]]
 */
class MetricConsumerSpec extends WordSpec with ShouldMatchers with MockitoSugar {

  trait MockQueue {
    val mockQueue = mock[MetricQueue]
  }

  trait NoMetricsAvailable extends MockQueue {
    val rootDataPath: Path = Files.createTempDirectory("metrics-")
    val metricConsumer = new MetricConsumer(rootDataPath.toFile, mockQueue)
  }

  trait MetricWriters extends NoMetricsAvailable {
    val oneLevelTmpDataDir = Files.createTempDirectory(rootDataPath,"one-level-")
    val twoLevelTmpDataDir = Files.createTempDirectory(oneLevelTmpDataDir,"two-level-")

    val writers = Map(
      "root" -> List(MetricFileQueue.getInstance(rootDataPath.toFile()), MetricFileQueue.getInstance(rootDataPath.toFile())),
      "one" -> List(MetricFileQueue.getInstance(oneLevelTmpDataDir.toFile), MetricFileQueue.getInstance(oneLevelTmpDataDir.toFile)),
      "two" -> List(MetricFileQueue.getInstance(oneLevelTmpDataDir.toFile), MetricFileQueue.getInstance(oneLevelTmpDataDir.toFile))
    )
  }

  trait OneMetricAvailable extends MetricWriters {
    writers("root")(0).create("AnyID","AnyMetric",1,1,Metric.RecordType.AGGREGATE)
    writers("root")(1).create("AnyID","AnyMetric",1,1,Metric.RecordType.AGGREGATE)
  }

  "A Metric Consumer" should {

    "emit no metrics when no metrics were written to disk " in new NoMetricsAvailable {
      metricConsumer.run()
      verify(mockQueue, never()).create(Matchers.any[MetricIdParts](), Matchers.any[MetricIdParts](), Matchers.anyLong(),
        Matchers.anyLong(), Matchers.any[Metric.RecordType]())
    }

//    TODO Complete Tests.
//    It is very difficult to test balboa agent with metric consumer.  Mainly because Metric Consumer uses a very
//    time based approach to handle which files to "grovel" or read.  This needs to to handled in later releases when we
//    have a more sustainable test driven model.

    "emit 1 metrics when 1 metrics was written to disk " in new OneMetricAvailable {
//      metricConsumer.run()
//      verify(mockQueue, times(1)).create(Matchers.any[MetricIdParts](), Matchers.any[MetricIdParts](), Matchers.anyLong(),
//        Matchers.anyLong(), Matchers.any[Metric.RecordType]())
    }

    "emits multiple metrics when multiple metrics were written to disk " in new MetricWriters {

    }
  }
  
}
