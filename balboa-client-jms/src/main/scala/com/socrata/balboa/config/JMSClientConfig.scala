package com.socrata.balboa.config

import com.socrata.balboa.metrics.config.{Configuration, Keys}

/**
 * JMS Client configuration.
 */
trait JMSClientConfig extends CoreClientConfig {

  /**
   * @return The address and port of the ActiveMQ Server to communicate to.
   */
  def activemqServer: String = Configuration.get().getString(Keys.JMS_ACTIVEMQ_SERVER, "failover:tcp://127.0.0.1:61616")

  /**
   * @return ActiveMQ Queue to publish to.
   */
  def activemqQueue: String = Configuration.get().getString(Keys.JMS_ACTIVEMQ_QUEUE)

  def activemqUser: Option[String] = Option(Configuration.get().getString(Keys.JMS_ACTIVEMQ_USER, null))

  def activemqPassword: Option[String] = Option(Configuration.get().getString(Keys.JMS_ACTIVEMQ_PASSWORD, null))

  def bufferSize: Int = Configuration.get().getInt(Keys.JMS_ACTIVEMQ_MAX_BUFFER_SIZE, 1)

}

object JavaJMSClientConfig extends JMSClientConfig
