package com.socrata.balboa.config

import com.socrata.balboa.config.ClientType.ClientType
import com.socrata.balboa.metrics.config.{Configuration, Keys}
import org.slf4j.LoggerFactory

/**
 * All inclusive Configuration file
 */
object DispatcherConfig {

  private val Log = LoggerFactory.getLogger("DispatcherConfig")

  /**
   * @return Client Types parsed from configuration.
   */
  def clientTypes: Array[ClientType] = {
    val clientStrings: Array[String] = Configuration.get().getString(Keys.DISPATCHER_CLIENT_TYPES).split(',')

    // Notify that we found an invalid Client Type with a graceful log message
    clientStrings.filterNot(s => isClientType(s)) match {
      case a: Array[String] if a.nonEmpty => Log.warn(s"$a not a valid client type, acceptable client " +
        s"types ${ClientType.values}")
      case _ =>
    }

    clientStrings.filter(s => isClientType(s)).map(s => ClientType.withName(s))
  }

  private def isClientType(s: String): Boolean = ClientType.values.exists(t => t.toString.equals(s))
}


