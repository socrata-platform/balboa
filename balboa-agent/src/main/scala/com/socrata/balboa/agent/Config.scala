package com.socrata.balboa.agent

import java.io.File

import com.socrata.balboa.config.JMSClientConfig
import com.socrata.balboa.metrics.config.Configuration

object Keys {

  lazy val DATA_DIRECTORY = "balboa.agent.data.dir"

  lazy val SLEEP_MS = "balboa.agent.sleeptime"

  lazy val INITIAL_DELAY = "balboa.agent.initialdelay"

}

/**
 * Parent Configuration class for all Client Configurations.  This class encapsulates core functionality that can be
 * shared between all clients.
 */
trait Config extends JMSClientConfig {

  // Data Directory configuration and

  def dataDirectory(defaultFile: File = null): File = Configuration.get().getFile(Keys.DATA_DIRECTORY, defaultFile)

  def interval(defaultTime: Long = 1000): Long = Configuration.get().getLong(Keys.SLEEP_MS, defaultTime)

  def initialDelay(defaultDelay: Long = 0): Long = Configuration.get().getLong(Keys.INITIAL_DELAY, defaultDelay)

}

/**
 * Java Configuration object for Balboa Agent.
 */
class ConfigForJava extends Config
