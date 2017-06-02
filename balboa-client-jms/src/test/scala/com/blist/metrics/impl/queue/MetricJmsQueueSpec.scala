package com.blist.metrics.impl.queue

import java.util
import javax.jms._

import com.socrata.balboa.metrics.{Metric, Metrics}
import com.socrata.balboa.metrics.impl.JsonMessage
import com.socrata.metrics.{Fluff, MetricQueue}
import org.mockito.ArgumentMatcher
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, WordSpec}
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import scala.collection.immutable.IndexedSeq

class MetricJmsQueueSpec extends WordSpec with Matchers with MockitoSugar {

  class JsonMessageMatcher(expected: JsonMessage) extends ArgumentMatcher[String] {
    def matches(argument: String): Boolean = {
      val parsedMessage = JsonMessage(argument)
      println(s"Parsed ${parsedMessage}")
      println(s"Expected: ${expected}")
      println(parsedMessage.getMetrics == expected.getMetrics)
      println(JsonMessage(argument).equals(expected))
      parsedMessage.getMetrics == expected.getMetrics && JsonMessage(argument) == expected
    }
  }

  trait AllAbsoluteQueueSetup extends QueueSetup {
    val testMetrics: IndexedSeq[MetricRecord] = IndexedSeq(
      MetricRecord(1496268557542l, "3", "num-metadata-completed", 21,  Metric.RecordType.ABSOLUTE),
      MetricRecord(1496268557542l, "3", "num-metadata-available", 114,  Metric.RecordType.ABSOLUTE),
      MetricRecord(1496268557542l, "1", "num-metadata-completed", 5339,  Metric.RecordType.ABSOLUTE),
      MetricRecord(1496268557542l, "1", "num-metadata-available", 20916,  Metric.RecordType.ABSOLUTE),
      MetricRecord(1496268557542l, "14", "num-metadata-completed", 41,  Metric.RecordType.ABSOLUTE),
      MetricRecord(1496268557542l, "14", "num-metadata-available", 270,  Metric.RecordType.ABSOLUTE)
    )
  }

  trait AllAggregateQueueSetup extends QueueSetup {
    val testMetrics: IndexedSeq[MetricRecord] = IndexedSeq(
      MetricRecord(1496268557542l, "3", "num-metadata-total", 21,  Metric.RecordType.ABSOLUTE),
      MetricRecord(1496268557542l, "3", "num-metadata-total", 114,  Metric.RecordType.ABSOLUTE),
      MetricRecord(1496268557542l, "1", "num-metadata-total", 5339,  Metric.RecordType.ABSOLUTE),
      MetricRecord(1496268557542l, "1", "num-metadata-total", 20916,  Metric.RecordType.ABSOLUTE),
      MetricRecord(1496268557542l, "14", "num-metadata-total", 41,  Metric.RecordType.ABSOLUTE),
      MetricRecord(1496268557542l, "14", "num-metadata-total", 270,  Metric.RecordType.ABSOLUTE)
    )
  }

  trait MixedQueueSetup extends QueueSetup {
    val testMetrics: IndexedSeq[MetricRecord] = IndexedSeq(
      MetricRecord(1496268557542l, "3", "num-metadata-completed", 21,  Metric.RecordType.ABSOLUTE),
      MetricRecord(1496268557542l, "3", "num-metadata-available", 114,  Metric.RecordType.ABSOLUTE),
      MetricRecord(1496268557542l, "3", "num-metadata-total", 21,  Metric.RecordType.AGGREGATE),
      MetricRecord(1496268557542l, "3", "num-metadata-total", 114,  Metric.RecordType.AGGREGATE),
      MetricRecord(1496268557542l, "1", "num-metadata-completed", 5339,  Metric.RecordType.ABSOLUTE),
      MetricRecord(1496268557542l, "1", "num-metadata-available", 20916,  Metric.RecordType.ABSOLUTE),
      MetricRecord(1496268557542l, "1", "num-metadata-total", 5339,  Metric.RecordType.AGGREGATE),
      MetricRecord(1496268557542l, "1", "num-metadata-total", 20916,  Metric.RecordType.AGGREGATE),
      MetricRecord(1496268557542l, "14", "num-metadata-completed", 41,  Metric.RecordType.ABSOLUTE),
      MetricRecord(1496268557542l, "14", "num-metadata-available", 270,  Metric.RecordType.ABSOLUTE),
      MetricRecord(1496268557542l, "14", "num-metadata-total", 41,  Metric.RecordType.AGGREGATE),
      MetricRecord(1496268557542l, "14", "num-metadata-total", 270,  Metric.RecordType.AGGREGATE)
    )
  }

  abstract trait QueueSetup {
    val mockQueue: Queue = mock[Queue]

    val mockProducer: MessageProducer = mock[MessageProducer]

    val mockSession: Session = mock[Session]

    when(mockSession.createQueue("not-a-real-queue")).thenReturn(mockQueue)
    when(mockSession.createProducer(mockQueue)).thenReturn(mockProducer)

    val mockConnection: Connection = mock[Connection]

    when(mockConnection.createSession(false, Session.AUTO_ACKNOWLEDGE)).thenReturn(mockSession)

    case class MetricRecord(timestamp: Long, entityId: String, name: String, value: Number, recordType: Metric.RecordType)

    def testMetrics: IndexedSeq[MetricRecord]

    val metricWithZeroValue = new Metric()
    metricWithZeroValue.setValue(0)
    metricWithZeroValue.setType(Metric.RecordType.AGGREGATE)

    def run() = {

      lazy val bucketedMetrics = testMetrics.foldLeft(Map.empty[(Long, String), Metrics])((accum, metric) => {
        accum.get((1496268480000l, metric.entityId)) match {
          case Some(metrics: Metrics) => {
            val metricObj = new Metric()

            val value: Number = metric.recordType match {
              case Metric.RecordType.ABSOLUTE => metric.value
              case Metric.RecordType.AGGREGATE => {
                val storedValue: Number = metrics.getOrDefault(metric.name, metricWithZeroValue).getValue
                storedValue.longValue + metric.value.longValue
              }
              case _ => 0
            }

            metricObj.setValue(value)

            metricObj.setType(metric.recordType)
            accum ++ Map[(Long, String), Metrics]((metric.timestamp, metric.entityId) -> {
              metrics.put(metric.name, metricObj)
              new Metrics(metrics)
            })
          }
          case None => {
            val metricObj = new Metric()

            metricObj.setValue(metric.value)
            metricObj.setType(metric.recordType)

            accum ++ Map[(Long, String), Metrics]((1496268480000l, metric.entityId) -> {
              val newHash = new util.HashMap[String,Metric]()
              newHash.put(metric.name, metricObj)
              new Metrics(newHash)
            })
          }
        }
      })

      lazy val queue = new MetricJmsQueue(mockConnection, "not-a-real-queue", 20000)

      testMetrics.foreach(r => {
        println(s"Inserting $r")
        queue.create(Fluff(r.entityId), Fluff(r.name), r.value.longValue, r.timestamp, r.recordType)
      })

      queue.flushWriteBuffer()


      Seq((1496268480000l,"3"),(1496268480000l,"1"),(1496268480000l,"14")).foreach((bucketId) => {
        val (timestamp, entityId) = bucketId
        bucketedMetrics.get((timestamp, entityId)) match {
          case Some(metrics) => {
            println(s"$bucketId $metrics")

            val msg = new JsonMessage
            msg.setEntityId(entityId)
            msg.setMetrics(metrics)
            msg.setTimestamp(timestamp)
            val bytes = msg.serialize

            verify(mockSession, times(1)).createTextMessage(argThat[String](new JsonMessageMatcher(msg)))
          }
          case None => {
            fail("Entity id should exist")
          }
        }
      })
    }
  }

  val agg = Metric.RecordType.AGGREGATE
  val abs = Metric.RecordType.ABSOLUTE
  val granularity = MetricQueue.AGGREGATE_GRANULARITY

  "when queued with all absolute metrics" should {
    "send those metrics over ActiveMQ" in new AllAbsoluteQueueSetup {
      run
    }
  }

  "when queued with all aggregate metrics" should {
    "send those metrics over ActiveMQ" in new AllAggregateQueueSetup {
      run
    }
  }

  "when queued with mixed absolute and aggregate metrics" should {
    "send those metrics over ActiveMQ" in new MixedQueueSetup {
      run
    }
  }
}
