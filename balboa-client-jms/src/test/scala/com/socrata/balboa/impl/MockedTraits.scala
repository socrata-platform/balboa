package com.socrata.balboa.impl

import com.socrata.balboa.metrics.Message
import com.socrata.metrics.components.MessageQueueComponent
import org.apache.commons.logging.LogFactory

import scala.collection.mutable.Queue

trait TestMessageQueueComponent extends MessageQueueComponent {
  val testLog = LogFactory.getLog(classOf[MessageQueue])

  val dumpingQueue = Queue.empty[Message]

  class MessageQueue() extends MessageQueueLike {

    def start() {}
    def send(msg:Message) { dumpingQueue.enqueue(msg) }
    def stop() { testLog.info("Shutdown BalboaClient") }
  }

  override def MessageQueue() = new MessageQueue()
}
