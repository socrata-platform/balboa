package com.socrata.balboa.common.kafka

import java.util.Properties

/**
 * Constants that will remain consistent between all consumer's and producers.
 */
object Constants {

  // See: Kafka Producer Config: producer.type
  val PRODUCER_TYPE = "sync"

  // See: Kafka Producer Config: request.required.acks
  // Verify that one broker gets the message
  val REQUEST_REQUIRED_ACKS = "1"

  /**
   * While this returns the base configuration for producers, the returned property
   *  instance still require metadata.broker.list, serializer.class, key.serializer.class.
   *
   * @return Default Properties for producers.
   */
  def producerDefaultProps: Properties = {
    val p = new Properties()
    p.setProperty("request.required.acks", REQUEST_REQUIRED_ACKS)
    p.setProperty("producer.type", PRODUCER_TYPE)
    // Add more default producer configs here...
    p
  }

}
