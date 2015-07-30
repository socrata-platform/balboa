package com.socrata.balboa.impl

import com.socrata.balboa.common.config.{Configuration, Keys}
import com.socrata.balboa.impl.MetricLoggerToDispatcherSetup.FakeQueueComponents
import com.socrata.metrics.collection.LinkedBlockingPreBufferQueue
import com.socrata.metrics.components.{MessageQueueComponent, MetricEnqueuer}
import org.scalatest.{BeforeAndAfter, WordSpec}

/**
 * Unit test for [[MetricLoggerToDispatcher]].
 */
class MetricLoggerToDispatcherSpec extends WordSpec
with BeforeAndAfter with MetricLoggerToDispatcher with FakeQueueComponents {

  "A MetricLoggerToDispatcher" should {

    "eventually send a metric to all internal components" in {
//      TODO Fix unreliable dependency on waiting.
//      val m = MetricLogger()
//      for (i <- 1 to 10) {
//        m.logMetric(s"entity$i", s"name$i",i, i, RecordType.AGGREGATE)
//      }
//      m.metricDequeuer.actualBuffer.flush()
//      Thread.sleep(5000) // This is due to the executor
//      assert(fakeComponents.forall(_.queue.size == 10))
    }
  }

  override protected def before(fun: => Any): Unit = {
    super.before(fun)
    Configuration.get().put(Keys.DISPATCHER_CLIENT_TYPES, "jms,kafka")
  }

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

object MetricLoggerToDispatcherSetup {

  trait FakeQueueComponents {
    val fakeComponents: Seq[FakeQueueComponent] = Seq(
      FakeQueueComponent(), FakeQueueComponent(), FakeQueueComponent())
  }

}
