package com.socrata.balboa.agent

import java.io.File

import com.socrata.balboa.config.JMSClientConfig
import com.socrata.balboa.metrics.config.Configuration

import scala.concurrent.duration._

object Keys {

  lazy val DataDirectory = "balboa.agent.data.dir"

  lazy val SleepMs = "balboa.agent.sleeptime"

  lazy val InitialDelay = "balboa.agent.initialdelay"

  lazy val TransportType = "balboa.agent.transport.type"

  lazy val BalboaHttpUrl = "balboa.agent.balboa.http.url"

  lazy val BalboaHttpTimeoutMs = "balboa.agent.balboa.http.timeout"

}

/**
 * Parent Configuration class for all Client Configurations.  This class encapsulates core functionality that can be
 * shared between all clients.
 */
trait Config extends JMSClientConfig {
  def dataDirectory(defaultFile: File = null): File // scalastyle:ignore
    = Configuration.get().getFile(Keys.DataDirectory, defaultFile)

  def interval(defaultTime: Long = 1000): Long // scalastyle:ignore
    = Configuration.get().getLong(Keys.SleepMs, defaultTime)

  def initialDelay(defaultDelay: Long = 0): Long = Configuration.get().getLong(Keys.InitialDelay, defaultDelay)

  def transportType(defaultMethod: TransportType = Mq): TransportType =
    Configuration.get().getString(Keys.TransportType, defaultMethod.toString) match {
      case "HTTP" => Http
      case "MQ" => Mq
      case _ => defaultMethod
    }

  def balboaHttpUrl: Option[String] =
    Option(Configuration.get().getString(Keys.BalboaHttpUrl))

  def balboaHttpTimeout(defaultTimeout: Duration): Duration =
    Configuration.get().getLong(Keys.BalboaHttpTimeoutMs, defaultTimeout.toMillis).millis
}

/**
 * Java Configuration object for Balboa Agent.
 */
class ConfigForJava extends Config
