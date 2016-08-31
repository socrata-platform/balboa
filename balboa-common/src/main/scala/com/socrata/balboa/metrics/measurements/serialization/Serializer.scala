package com.socrata.balboa.metrics.measurements.serialization

import java.io.IOException

import com.socrata.balboa.metrics.config.ConfigurationException
import com.typesafe.config.Config

trait Serializer {

  @throws(classOf[IOException])
  def serialize(value: AnyRef): Array[Byte]

  @throws(classOf[IOException])
  def deserialize(serialized: Array[Byte]): AnyRef
}

object Serializer {
  def fromConfig(conf: Config): Serializer = {
    conf.getString("balboa.serializer") match {
      case "json" => new JsonSerializer()
      case "protobuf" => new ProtocolBuffersSerializer()
      case other: Any => throw new ConfigurationException("Invalid serializer configured: " + other)
    }
  }
}
