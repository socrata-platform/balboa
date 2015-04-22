package com.socrata.balboa.config

import com.socrata.balboa.metrics.config.{Configuration, Keys}

/**
 * JMS Client configuration.
 */
object JMSClientConfig extends CoreClientConfig {

  /**
   * @return The address and port of the ActiveMQ Server to communicate to.
   */
  def activemqServer: String = Configuration.get().getString(Keys.JMS_ACTIVEMQ_QUEUE,
    "failover:tcp://120.0.0.1:61616")

  /**
   * @return ActiveMQ Queue to publish to.
   */
  def activemqQueue: String = Configuration.get().getString(Keys.JMS_ACTIVEMQ_QUEUE)

}
