package com.socrata.balboa.impl

import javax.jms.Session

import com.socrata.balboa.common.logging.BalboaLogging
import com.socrata.balboa.metrics.Message
import com.socrata.metrics.components.{EmergencyFileWriterComponent, MessageQueueComponent}
import org.apache.activemq.ActiveMQConnectionFactory


trait ActiveMQueueComponent extends MessageQueueComponent with BalboaLogging {
  self: ServerInformation with EmergencyFileWriterComponent =>

  class MessageQueue extends MessageQueueLike {
    val factory = new ActiveMQConnectionFactory(activeServer)
    factory.setUseAsyncSend(true)
    val connection = factory.createConnection()
    val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val producer = session.createProducer(session.createQueue(activeQueue))
    val fileWriter = EmergencyFileWriter(backupFile)

    def start() { connection.start }

    def stop() {
      connection.close()
      fileWriter.close()
      logger.info("Shutdown BalboaClient")
    }

    def send(msg:Message) = {
      try {
        producer.send(session.createTextMessage(new String(msg.serialize())))
      } catch {
        case e:Exception =>
          logger.error(e.getMessage)
          logger.error("Sending message to ActiveMQ failed (see previous error); writing message to file " + backupFile)
          fileWriter.send(msg)
      }
    }
  }

  def MessageQueue() = new MessageQueue()
}
