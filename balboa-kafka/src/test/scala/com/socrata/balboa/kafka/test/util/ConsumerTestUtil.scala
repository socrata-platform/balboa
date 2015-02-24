package com.socrata.balboa.kafka.test.util

import java.util.Calendar

import com.socrata.balboa.metrics.{Metrics, Metric}
import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.metrics.impl.JsonMessage
import kafka.message.{Message, MessageAndMetadata}
import kafka.serializer.DefaultDecoder

/**
 * Utility class for Consumers.
 */
object ConsumerTestUtil {

  // Helper methods

  def message(key: String, message: String):MessageAndMetadata[Array[Byte], Array[Byte]] = {
    val k: Array[Byte] = if (key == null) null else key.toCharArray.map(_.toByte)
    val m: Array[Byte] = if (message == null) null else message.toCharArray.map(_.toByte)
    new MessageAndMetadata[Array[Byte], Array[Byte]]("test_topic", 1, new Message(m, k),
      0, new DefaultDecoder(), new DefaultDecoder())
  }


  def message(json: JsonMessage): MessageAndMetadata[Array[Byte], Array[Byte]] = message(null, json.toString)

  def testJSONMetric(name: String, num: Number): JsonMessage = {
    val metric = new Metric()
    metric.setType(RecordType.ABSOLUTE)
    metric.setValue(num)
    val metrics = new Metrics()
    metrics.put(name, metric)
    testJSONMetric(metrics)
  }

  def testJSONMetric(metrics: Metrics = new Metrics()): JsonMessage = {
    val json = new JsonMessage()
    json.setEntityId("test_id")
    json.setTimestamp(Calendar.getInstance().getTime.getTime)
    json.setMetrics(metrics)
    json
  }

  def testStringMessage(key: Array[Byte], message: Array[Byte]): Either[String, String] = (key, message)
  match {
    case (k: Array[Byte], m: Array[Byte]) =>
      Right(combine(new String(k.map(_.toChar)), new String(m.map(_.toChar))))
    case (_, m: Array[Byte]) =>
      Right(combine(null, new String(m.map(_.toChar))))
    case (k: Array[Byte], _) =>
      Right(combine(new String(k.map(_.toChar)), null))
    case _=> Left("Cannot convert null reference to byte array")
  }

  def combine(k: String, m: String): String = s"$k:$m"

}
