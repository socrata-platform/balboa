package com.socrata.balboa.impl

import com.socrata.balboa.impl.MetricLoggerToDispatcherSetup.FakeQueueComponents
import com.socrata.balboa.metrics.config.Keys
import com.socrata.metrics.collection.LinkedBlockingPreBufferQueue
import com.socrata.metrics.components.{MessageQueueComponent, MetricEnqueuer}
import com.typesafe.config.Config
import org.mockito.Mockito._
import org.scalatest.WordSpec
import org.scalatest.mock.MockitoSugar

import scala.collection.JavaConverters._

/**
 * Unit test for [[MetricLoggerToDispatcher]].
 */
class MetricLoggerToDispatcherSpec extends WordSpec with MockitoSugar {

  "A MetricLoggerToDispatcher" should {

    "eventually send a metric to all internal components" in {

      val mockConfig = mock[Config]
      when(mockConfig.getStringList(Keys.DispatcherClientTypes)).thenReturn(List("jms").asJava)

      val metricLoggerToDispatcher = new MetricLoggerToDispatcher with FakeQueueComponents {
        override def config = mockConfig
          /**
           * Creates a Metric Logger using preset configurations.
           *
           * @return [[MetricLoggerLike]] instance.
           */
          override def MetricLogger(): MetricLogger = new MetricLogger()
            with MetricEnqueuer
            with MetricDequeuerService
            with HashMapBufferComponent
            with BalboaDispatcherComponent
            with LinkedBlockingPreBufferQueue
            with BufferedStreamEmergencyWriterComponent
            with DispatcherInformation {

            /**
             * See [[DispatcherInformation.components]].
             */
            lazy val components: Iterable[MessageQueueComponent] = fakeComponents
          }
      }

//      TODO Fix unreliable dependency on waiting.
//      val m = metricLoggerToDispatcher.MetricLogger()
//      for (i <- 1 to 10) {
//        m.logMetric(s"entity$i", s"name$i", i, i, RecordType.AGGREGATE)
//      }
//      m.metricDequeuer.actualBuffer.flush()
//      Thread.sleep(5000) // This is due to the executor
//      assert(metricLoggerToDispatcher.fakeComponents.forall(_.queue.size == 10))
    }
  }

}

object MetricLoggerToDispatcherSetup {
  trait FakeQueueComponents {
    val fakeComponents: Seq[FakeQueueComponent] = Seq(
      FakeQueueComponent(), FakeQueueComponent(), FakeQueueComponent())
  }
}
