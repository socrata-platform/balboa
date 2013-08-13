package com.socrata.metrics.components

import com.socrata.balboa.metrics.Message
import java.io.File

trait MessageQueueComponent {
  type MessageQueue <: MessageQueueLike
  trait MessageQueueLike {
    def start():Unit
    def send(msg:Message):Unit
    def stop():Unit
  }

  def MessageQueue():MessageQueue
}

trait EmergencyFileWriterComponent {
  type EmergencyFileWriter <: EmergencyFileWriterLike
  trait EmergencyFileWriterLike {
    def send(msg:Message):Unit
    def close():Unit
  }

  def EmergencyFileWriter(file:File):EmergencyFileWriter
}
