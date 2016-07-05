package com.socrata.balboa.impl

import javax.jms.Session

import com.socrata.balboa.metrics.Message
import com.socrata.metrics.components.{EmergencyFileWriterComponent, MessageQueueComponent}
import com.typesafe.scalalogging.StrictLogging
import org.apache.activemq.ActiveMQConnectionFactory

trait ActiveMQueueComponent extends MessageQueueComponent with StrictLogging {
  self: ServerInformation with EmergencyFileWriterComponent =>

  class MessageQueue extends MessageQueueLike {
    val factory = new ActiveMQConnectionFactory(activeServer)
    factory.setUseAsyncSend(true)
    val connection = factory.createConnection()
    val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val producer = session.createProducer(session.createQueue(activeQueue))
    val fileWriter = EmergencyFileWriter(backupFile)

    def start(): Unit = connection.start()

    def stop(): Unit = {
      connection.close()
      fileWriter.close()
      logger info "Shutdown BalboaClient"
    }

    def send(msg:Message): Unit = {
      try {
        producer.send(session.createTextMessage(new String(msg.serialize())))
      } catch {
        case e:Exception =>
          logger.error("Sending message to ActiveMQ failed (see previous error); writing message to file " + backupFile, e)
          fileWriter.send(msg)
      }
    }
  }

  def MessageQueue(): MessageQueueLike = new MessageQueue()
}
