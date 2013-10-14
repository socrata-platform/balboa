package com.socrata.metrics.impl

import com.socrata.metrics.components.MessageQueueComponent
import com.socrata.balboa.metrics.Message
import scala.collection.mutable.Queue
import org.apache.commons.logging.LogFactory

trait TestMessageQueueComponent extends MessageQueueComponent {
  val testLog = LogFactory.getLog(classOf[MessageQueue])

  class MessageQueue() extends MessageQueueLike {
    val dumpingQueue = Queue.empty[Message]

    def start() {}
    def send(msg:Message) { dumpingQueue.enqueue(msg) }
    def stop() { testLog.info("Shutdown BalboaClient") }
  }

  def MessageQueue() = new MessageQueue()
}
