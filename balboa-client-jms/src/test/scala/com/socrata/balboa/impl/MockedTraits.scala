package com.socrata.balboa.impl

import com.socrata.balboa.common.logging.BalboaLogging
import com.socrata.balboa.metrics.Message
import com.socrata.metrics.components.MessageQueueComponent

import scala.collection.mutable.Queue

trait TestMessageQueueComponent extends MessageQueueComponent with BalboaLogging {

  val testLog = logger

  val dumpingQueue = Queue.empty[Message]

  class MessageQueue() extends MessageQueueLike {

    def start() {}
    def send(msg:Message) { dumpingQueue.enqueue(msg) }
    def stop() { testLog.info("Shutdown BalboaClient") }
  }

  override def MessageQueue() = new MessageQueue()
}
