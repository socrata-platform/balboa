package com.socrata.balboa.agent

import java.io.File
import java.nio.file.Paths

import com.socrata.balboa.config.JMSClientConfig
import com.socrata.balboa.metrics.config.Keys
import com.typesafe.config.Config

import scala.concurrent.duration._
import scala.util.Try

/**
 * Parent Configuration class for all Client Configurations.  This class encapsulates core functionality that can be
 * shared between all clients.
 */
class AgentConfig(conf: Config) extends JMSClientConfig(conf) {

  val dataDirectory: File = Paths.get(conf.getString(Keys.DataDirectory)).toFile

  val interval: Long = conf.getLong(Keys.SleepMs)

  val initialDelayMs: Long = conf.getLong(Keys.InitialDelayMs)

  val transportType: TransportType =
    conf.getString(Keys.TransportType) match {
      case "HTTP" => Http
      case "MQ" => Mq
      case transportType => // scalastyle:ignore
        throw new IllegalArgumentException("Invalid transport type: " + transportType)
    }

  val balboaHttpUrl: String = conf.getString(Keys.BalboaHttpUrl)
  val balboaHttpTimeout: Duration = conf.getLong(Keys.BalboaHttpTimeoutMs).millis
  val balboaHttpMaxRetryWait: Duration = conf.getLong(Keys.BalboaHttpMaxRetryWaitMs).millis

  val activemqCloseTimeout: Int = conf.getInt(Keys.JMSActiveMQCloseTimeout)
}
