package com.socrata.balboa.tickertape

import java.io.File

import com.socrata.balboa.metrics.config.Configuration

object Keys {

  lazy val DATA_DIRECTORY = "balboa.tmp.data.dir"

  lazy val SLEEP_MS = "balboa.tmp.sleeptime"

  lazy val BATCH_SIZE = "balboa.tmp.batchsize"

}

/**
 * Parent Configuration class for all Client Configurations.  This class encapsulates core functionality that can be
 * shared between all clients.
 */
trait Config {

  // Data Directory configuration and

  def dataDirectory(defaultFile: File = null) = Configuration.get().getFile(Keys.DATA_DIRECTORY, defaultFile)

  def sleepTime(defaultTime: Long = 1000): Long = Configuration.get().getLong(Keys.SLEEP_MS, defaultTime)

  def batchSize(defaultSize: Int = 1): Int = Configuration.get().getInt(Keys.BATCH_SIZE, defaultSize)

}

/**
 * Java Configuration object for Balboa Agent.
 */
class ConfigForJava extends Config
