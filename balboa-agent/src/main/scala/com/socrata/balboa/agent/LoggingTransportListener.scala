package com.socrata.balboa.agent

import java.io.IOException

import com.typesafe.scalalogging.StrictLogging
import org.apache.activemq.transport.TransportListener

class LoggingTransportListener extends TransportListener with StrictLogging {
    // The onCommand handler seems to be a routine handler. No additional error
    // reporting information is available by overriding it.
    override def onCommand(command: scala.Any): Unit = {}

    override def onException(error: IOException): Unit =
      logger error("Problem with ActiveMQ connection.", error)

    // This method appears to be invoked when the library is first initializing
    // as well. So this log message will appear after the call to `.start` down
    // below.
    override def transportInterupted(): Unit =
      logger warn "ActiveMQ connection is being closed or has suffered an interruption from which it hopes to recover."

    override def transportResumed(): Unit =
      logger warn "ActiveMQ connection is starting up or has resumed after an interruption."
}
