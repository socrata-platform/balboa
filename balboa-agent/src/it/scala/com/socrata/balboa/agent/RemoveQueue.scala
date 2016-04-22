package com.socrata.balboa.agent

import javax.jms.{ExceptionListener, JMSException, Session}

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.activemq.command.ActiveMQDestination
import org.apache.activemq.{ActiveMQConnection, ActiveMQConnectionFactory}

/**
  * A standalone program used to delete the ActiveMQ queue that is created during
  * integration testing. A new queue is created each time the integration tests
  * are run to avoid colliding with other simultaneous integration tests running
  * against the same ActiveMQ server, so it is necessary to clean up the queue
  * when testing is complete.
  */
object RemoveQueue extends App with StrictLogging {

  logger info "Initializing ActiveMQ connection. (This is setting the username, password and destination.)"
  val connectionFactory: ActiveMQConnectionFactory = new ActiveMQConnectionFactory(Config.activemqServer)
  val connection = connectionFactory.createConnection().asInstanceOf[ActiveMQConnection]

  connection.setExceptionListener(new ExceptionListener {
    override def onException(exception: JMSException): Unit =
      logger error(s"ERROR: There was some kind of problem with the ActiveMQ connection.", exception)
  })
  logger info "Connecting to ActiveMQ broker. (This is the actual TCP connection.)"
  connection.start()
  logger info s"Connected to ActiveMQ broker '${connection.getBrokerInfo}'."

  val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
  logger info s"Getting the destination queue ${Config.activemqQueue}"
  val destination = session.createQueue(Config.activemqQueue).asInstanceOf[ActiveMQDestination]
  logger info s"Removing the destination queue ${Config.activemqQueue}"
  try {
    connection.destroyDestination(destination)
  } catch {
    case e: Throwable => logger error("Unable to remove the queue.", e)
  }

  logger info "Closing the connection."
  connection.close()

  logger info "Done. Exiting..."
}
