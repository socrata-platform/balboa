package com.socrata.metrics.impl

import com.socrata.metrics.components.MessageQueueComponent
import com.socrata.balboa.metrics.Message
import scala.collection.mutable.Queue

trait TestMessageQueueComponent extends MessageQueueComponent {

  class MessageQueue() extends MessageQueueLike {
    val dumpingQueue = Queue.empty[Message]

    def start() {}
    def send(msg:Message) { dumpingQueue.enqueue(msg) }
    def stop() {}
  }

  def MessageQueue() = new MessageQueue()
}
