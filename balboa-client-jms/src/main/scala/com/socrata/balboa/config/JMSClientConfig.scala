package com.socrata.balboa.config

import com.socrata.balboa.metrics.config.Keys
import com.typesafe.config.Config

/**
 * JMS Client configuration.
 */
class JMSClientConfig(conf: Config) extends CoreClientConfig(conf) {

  /** The address and port of the ActiveMQ Server to communicate to. */
  val activemqServer: String = conf.getString(Keys.JMSActiveMQServer)

  /** ActiveMQ Queue to publish to. */
  val activemqQueue: String = conf.getString(Keys.JMSActiveMQQueue)

  val activemqUser: String = conf.getString(Keys.JMSActiveMQUser)

  val activemqPassword: String = conf.getString(Keys.JMSActiveMQPassword)

  val bufferSize: Int = conf.getInt(Keys.JMSActiveMQMaxBufferSize)
}
