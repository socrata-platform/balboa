package com.socrata.balboa.impl

import java.io.{BufferedOutputStream, File, FileOutputStream}

import com.socrata.balboa.common.Message
import com.socrata.metrics.components.EmergencyFileWriterComponent

import scala.collection.JavaConverters._

trait BufferedStreamEmergencyWriterComponent extends EmergencyFileWriterComponent {

  class EmergencyFileWriter(file:File) extends EmergencyFileWriterLike {
    val fileStream = new FileOutputStream(file, true)
    val stream = new BufferedOutputStream(fileStream)

    def send(msg:Message) = {
      val metrics = msg.getMetrics
      for (name <- metrics.keySet().asScala) {
        stream.write(0xff)

        stream.write(utf8(msg.getTimestamp.toString))
        stream.write(0xfe)

        stream.write(utf8(msg.getEntityId))
        stream.write(0xfe)

        stream.write(utf8(name))
        stream.write(0xfe)

        stream.write(utf8(metrics.get(name).getValue.toString))
        stream.write(0xfe)

        stream.write(utf8(metrics.get(name).getType.toString))
        stream.write(0xfe)

        stream.flush()
      }
    }

    def close() { stream.close() }

    def utf8(s:String) = s.getBytes("utf-8")

  }

  def EmergencyFileWriter(file:File) = new EmergencyFileWriter(file)
}
