package com.socrata.metrics.components

import java.io.File

import com.socrata.balboa.metrics.Message

trait MessageQueueComponent {
  type MessageQueue <: MessageQueueLike
  trait MessageQueueLike {

    /**
     * Initialize the message queue and prepares to recieve messages.
     */
    def start():Unit

    /**
     * Sends a message using the underlying queue.
     * @param msg Messsage to send.  Should not be null.
     */
    def send(msg:Message):Unit

    /**
     * Stops and destroys the underlying queue.
     */
    def stop():Unit
  }

  def MessageQueue():MessageQueueLike
}

trait EmergencyFileWriterComponent {
  type EmergencyFileWriter <: EmergencyFileWriterLike
  trait EmergencyFileWriterLike {
    def send(msg:Message):Unit
    def close():Unit
  }

  def EmergencyFileWriter(file:File):EmergencyFileWriter
}
