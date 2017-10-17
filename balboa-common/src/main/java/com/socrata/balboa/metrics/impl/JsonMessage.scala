package com.socrata.balboa.metrics.impl

import com.socrata.balboa.metrics.Message
import com.socrata.balboa.metrics.Metrics
import org.codehaus.jackson.annotate.JsonProperty
import org.codehaus.jackson.map.ObjectMapper
import java.io.ByteArrayOutputStream
import java.io.IOException

object JsonMessage {
  def apply(serialized: String): JsonMessage = {
    val message = new JsonMessage()
    message.deserialize(serialized)
    message
  }

  def apply(entityId: String, timestamp: Long, metrics: Metrics): JsonMessage = {
    val message = new JsonMessage()
    message.setEntityId(entityId)
    message.setTimestamp(timestamp)
    message.setMetrics(metrics)
    message
  }
}

case class JsonMessage() extends Message {

  @JsonProperty override def getEntityId: String = super.getEntityId

  @JsonProperty override def getTimestamp: Long = super.getTimestamp

  @JsonProperty override def getMetrics: Metrics = super.getMetrics

  @throws[IOException]
  override def serialize: Array[Byte] = {
    val mapper = new ObjectMapper
    val stream = new ByteArrayOutputStream
    mapper.writeValue(stream, this)
    stream.toByteArray
  }

  @throws[IOException]
  private[impl] def deserialize(serialized: String) = {
    val mapper = new ObjectMapper
    val other = mapper.readValue(serialized, classOf[JsonMessage])
    setEntityId(other.getEntityId)
    setMetrics(other.getMetrics)
    setTimestamp(other.getTimestamp)
  }

  override def toString: String = {
    val mapper = new ObjectMapper
    try mapper.writeValueAsString(this)
    catch {
      case e: IOException =>
        // For to String fail silently
        "JsonMessage{}"
    }
  }
}