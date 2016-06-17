package com.socrata.balboa.agent

import java.io.File

import com.socrata.balboa.config.JMSClientConfig
import com.socrata.balboa.metrics.config.Configuration

object Keys {

  lazy val DataDirectory = "balboa.agent.data.dir"

  lazy val SleepMs = "balboa.agent.sleeptime"

  lazy val InitialDelay = "balboa.agent.initialdelay"

  lazy val HttpOrMq = "balboa.agent.http.or.mq"

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

  def httpOrMQ(defaultMethod: HttpOrMq = HttpOrMq.HTTP): HttpOrMq =
    Configuration.get().getString(Keys.HttpOrMq, defaultMethod.toString) match {
      case "HTTP" => HttpOrMq.HTTP
      case "MQ" => HttpOrMq.MQ
      case _ => defaultMethod
    }
}

/**
 * Java Configuration object for Balboa Agent.
 */
class ConfigForJava extends Config
