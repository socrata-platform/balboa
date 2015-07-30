package com.socrata.balboa.agent

import java.io.File

import com.socrata.balboa.common.config.Configuration
import com.socrata.balboa.producer.config.JMSClientConfig

object Keys {

  lazy val DATA_DIRECTORY = "balboa.agent.data.dir"

  lazy val SLEEP_MS = "balboa.agent.sleeptime"

}

/**
 * Parent Configuration class for all Client Configurations.  This class encapsulates core functionality that can be
 * shared between all clients.
 */
trait Config extends JMSClientConfig {

  // Data Directory configuration and

  def dataDirectory(defaultFile: File = null) = Configuration.get().getFile(Keys.DATA_DIRECTORY, defaultFile)

  def sleepTime(defaultTime: Long = 1000): Long = Configuration.get().getLong(Keys.SLEEP_MS, defaultTime)

}

/**
 * Java Configuration object for Balboa Agent.
 */
class ConfigForJava extends Config
