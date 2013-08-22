package com.socrata.metrics.impl

import org.apache.activemq.ActiveMQConnectionFactory
import javax.jms.Session
import com.socrata.balboa.metrics.Message
import com.socrata.metrics.components.{EmergencyFileWriterComponent, MessageQueueComponent}
import org.apache.commons.logging.LogFactory
import java.io.File


trait ActiveMQueueComponent extends MessageQueueComponent {
  self: ServerInformation with EmergencyFileWriterComponent =>

  private val log = LogFactory.getLog(classOf[MessageQueue])

  class MessageQueue extends MessageQueueLike {
    val factory = new ActiveMQConnectionFactory(activeServer)
    factory.setUseAsyncSend(true)
    val connection = factory.createConnection()
    val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val producer = session.createProducer(session.createQueue(activeQueue))
    val fileWriter = EmergencyFileWriter(new File(backupFile))

    def start() { connection.start }

    def stop() {
      connection.close()
      fileWriter.close()
    }

    def send(msg:Message) = {
      try {
        producer.send(session.createTextMessage(new String(msg.serialize())))
      } catch {
        case e:Exception =>
          log.error(e)
          log.error("Sending message to ActiveMQ failed (see previous error); writing message to file " + backupFile)
          fileWriter.send(msg)
      }
    }
  }

  def MessageQueue() = new MessageQueue()
}
