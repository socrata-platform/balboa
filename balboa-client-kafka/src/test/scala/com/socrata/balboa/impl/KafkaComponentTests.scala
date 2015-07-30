package com.socrata.balboa.impl

import java.io.File

import com.socrata.balboa.common.codec.{BalboaMessageCodec, StringCodec}
import com.socrata.balboa.common.kafka.TestEnvironment
import com.socrata.balboa.common.{AddressAndPort, Message, MetricsTestStuff}
import com.socrata.integration.kafka.util.{BalboaClientTestUtils, BalboaMessageClientTestHarness}
import com.socrata.metrics.components.{EmergencyFileWriterComponent, MessageQueueComponent}
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable.Queue

/**
 * Kafka Component Tests.  See [[BalboaKafkaComponent]].  Test basic functionality of KafkaComponent.
 */
class KafkaComponentTests extends BalboaMessageClientTestHarness
with MetricsTestStuff.TestMessages with MetricLoggerToKafka {

  override val numPartitions: Int = TestEnvironment.NUM_PARTITIONS
  override val replicationFactor: Int = TestEnvironment.REPLICATION_FACTOR
  override val serverCount: Int = TestEnvironment.SERVER_COUNT

  override val producerCount: Int = 0 // Creating producers manually.
  override val consumerCount: Int = 1
  override val consumerGroupCount: Int = 1
  override val topic: String = "kafka_component_topic"
  val topic_internal = topic

  val emergencyQueue = Queue.empty[Message]
  var component: TestComponent = null

  override def setUp(): Unit = {
    super.setUp()
    component = new TestComponent() with BalboaKafkaComponent with QueueEmergencyWriter with KafkaProducerInformation {
      override def brokers: List[AddressAndPort] = AddressAndPort.parse(brokerList)
      override def file: File = File.createTempFile("emergency", "data")
      override def topic: String = topic_internal
    }
  }

  override def tearDown(): Unit = {
    emergencyQueue.clear()
    component.stop()
    super.tearDown()
  }

  @Test def testSendAndReceiveOfOneMessage(): Unit = {
    component.start()
    component.send(emptyMessage)
    validateConsumedMessages(emptyMessage)
  }

  @Test def testSendAndReceiveOfOneMessageWithoutInitialStart(): Unit = {
    component.send(emptyMessage)
    validateConsumedMessages(emptyMessage)
  }

  @Test def testSendWhenNoServersAreAvailabeWritesToEmergency(): Unit = {
    servers.foreach(s => {
      s.shutdown()
      s.awaitShutdown()
    })
    Range(0,3).foreach(_ => component.send(oneElemMessage))
    assertEquals("When the server is initially down all the messages will be written to the emergence file",
      3, emergencyQueue.size)
    assert(emergencyQueue.contains(oneElemMessage))
  }

  def validateConsumedMessages(m: Message*) = {
    val consumedMessages: List[(String,Message)] = BalboaClientTestUtils.getKeysAndMessages[String,Message](m.size,
      consumers(0).createMessageStreams[String, Message](Map((topic, 1)), new StringCodec, new BalboaMessageCodec()))
    // All the outgoing messages should be duplicated by a factor equal to the number of producers
    assertEquals("Number of total messages sent not equal to the number of total messages received.",
      m.size, consumedMessages.size)
    // Each outgoing message must be contained in the eventually consumed message.
    assert(m.forall(inMessage => {consumedMessages.unzip._2.contains(inMessage)}),
      "All the input messages should be contained in the output.")
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

}

/**
 * Component used for testing
 */
class TestComponent {
  self: MessageQueueComponent with EmergencyFileWriterComponent =>
  val messageQueue = self.MessageQueue()
  def start() = messageQueue.start()
  def stop() = messageQueue.stop()
  def send(msg: Message) = messageQueue.send(msg)
}


