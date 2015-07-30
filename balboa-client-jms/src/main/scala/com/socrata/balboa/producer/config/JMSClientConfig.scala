package com.socrata.balboa.producer.config

import com.socrata.balboa.common.config.{Configuration, Keys}

/**
 * JMS Client configuration.
 */
trait JMSClientConfig extends BalboaProducerConfig {

  /**
   * @return The address and port of the ActiveMQ Server to communicate to.
   */
  def activemqServer: String = Configuration.get().getString(Keys.JMS_ACTIVEMQ_SERVER, "failover:tcp://127.0.0.1:61616")

  /**
   * @return ActiveMQ Queue to publish to.
   */
  def activemqQueue: String = Configuration.get().getString(Keys.JMS_ACTIVEMQ_QUEUE)

}

class JavaJMSClientConfig extends JMSClientConfig
