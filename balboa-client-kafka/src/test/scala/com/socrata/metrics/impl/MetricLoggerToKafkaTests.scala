package com.socrata.metrics.impl

import java.io.File

import com.socrata.balboa.metrics.Message
import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.common.kafka.codec.{BalboaMessageCodec, StringCodec}
import com.socrata.balboa.common.kafka.util.{AddressAndPort, StagingAndRCEnvironment}
import com.socrata.integration.kafka.util.{BalboaClientTestUtils, BalboaMessageClientTestHarness}
import com.socrata.metrics.collection.LinkedBlockingPreBufferQueue
import com.socrata.metrics.components.{EmergencyFileWriterComponent, MetricEnqueuer}
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable.Queue

/**
 * Tests for Metric Logger to Kafka.
 */
class MetricLoggerToKafkaTests extends BalboaMessageClientTestHarness with MetricLoggerToKafka {

  override val numPartitions: Int = StagingAndRCEnvironment.NUM_PARTITIONS
  override val replicationFactor: Int = StagingAndRCEnvironment.REPLICATION_FACTOR
  override val serverCount: Int = StagingAndRCEnvironment.SERVER_COUNT

  override val producerCount: Int = 0
  override val consumerCount: Int = 1
  override val topic: String = "kafka_metric_logger_topic"

  private val agg = RecordType.AGGREGATE
  private val abs = RecordType.ABSOLUTE

  val emergencyQueue = Queue.empty[Message]

  var logger: MetricLogger = null

  override def setUp(): Unit = {
    super.setUp()
    logger = MetricLogger(brokerList, topic, "file_name_that_does_not_matter")
  }

  override def tearDown(): Unit = {
    logger.stop()
    emergencyQueue.clear()
    super.tearDown()
  }

  @Test def testLoggerSendsMessagesToIdealStateKafka(): Unit = {
    // TODO For some reason the consumer times out with 60 seconds but not 120.
    // These test are not deterministic enough.
    logger.logMetric("mike", "num_penguins", 5, 0L, agg)

    // Flush the buffer to write out all the messages
    logger.metricDequeuer.actualBuffer.flush()
    Thread.sleep(2000)
    val consumedMessages: List[(String,Message)] = BalboaClientTestUtils.getKeysAndMessages[String,Message](1,
      consumers(0).createMessageStreams[String, Message](Map((topic, 1)), new StringCodec, new BalboaMessageCodec()))
    // All the outgoing messages should be duplicated by a factor equal to the number of producers
    assertEquals("Number of total messages sent not equal to the number of total messages received.",
      1, consumedMessages.size)
    val m: Message = consumedMessages.unzip._2(0)
    assert(m.getMetrics.containsKey("num_penguins"), "Must contain Penguin sent Metric")
  }

  /**
   * Tracking emergencies locally.
   */
  trait QueueEmergencyWriter extends EmergencyFileWriterComponent {

    class EmergencyFileWriter(file:File) extends EmergencyFileWriterLike {

      // Store all files written as emergency
      override def send(msg: Message): Unit = emergencyQueue.enqueue(msg)
      override def close(): Unit = { /** NOOP */ }
    }

    override def EmergencyFileWriter(file: File): EmergencyFileWriter = new EmergencyFileWriter(file)
  }
  /**
   * See [[MetricLoggerToKafka.MetricLogger()]]
   */
  override def MetricLogger(s: String, t: String, b: String): MetricLogger =
    new MetricLogger() with MetricEnqueuer
      with MetricDequeuerService
      with HashMapBufferComponent
      with BalboaKafkaComponent
      with LinkedBlockingPreBufferQueue
      with QueueEmergencyWriter // Don't write the message out to a file
      with KafkaProducerInformation {
      lazy val brokers = AddressAndPort.parse(s)
      lazy val topic: String = t
      lazy val emergencyBackUpFile: String = b
    }
}


