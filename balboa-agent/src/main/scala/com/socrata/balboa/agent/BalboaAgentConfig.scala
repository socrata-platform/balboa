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
trait BalboaAgentConfig extends JMSClientConfig {

  // Data Directory configuration and

  def dataDirectory(defaultFile: File = null) = Configuration.get().getFile(Keys.DATA_DIRECTORY, defaultFile)

  def interval(defaultTime: Long = 1000): Long = Configuration.get().getLong(Keys.SLEEP_MS, defaultTime)

  def initialDelay(defaultDelay: Long = 0): Long = Configuration.get().getLong(Keys.INITIAL_DELAY, defaultDelay)

}

sealed case class TypeSafeConfig() extends BalboaAgentConfig {

  override def dataDirectory(defaultFile: File): File = super.dataDirectory(defaultFile)

  override def interval(defaultTime: Long): Long = super.interval(defaultTime)

  override def initialDelay(defaultDelay: Long): Long = super.initialDelay(defaultDelay)

  override def activemqServer: String = super.activemqServer

  override def activemqUser: Option[String] = super.activemqUser

  override def activemqQueue: String = super.activemqQueue

  override def activemqPassword: Option[String] = super.activemqPassword

  override def bufferSize: Int = super.bufferSize

  override def emergencyBackUpDir: File = super.emergencyBackUpDir

  override def emergencyBackUpFile(name: String): File = super.emergencyBackUpFile(name)
}

object BalboaAgentConfig {

  /**
    * Utilizes the [[Configuration]] class for supplying configuration.
    *
    * @return Configuration object for Balboa Agent.
    */
  def apply() = new BalboaAgentConfig {} // Non library breaking changes.

}

/**
 * Java Configuration object for Balboa Agent.
 */
class ConfigForJava extends BalboaAgentConfig
