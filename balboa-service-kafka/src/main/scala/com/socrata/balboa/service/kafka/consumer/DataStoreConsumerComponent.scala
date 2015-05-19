package com.socrata.balboa.service.kafka.consumer

import java.io.IOException

import com.socrata.balboa.metrics.Message
import com.socrata.balboa.metrics.data.DataStore

/**
 * Balboa Consumer's are inherited from [[KafkaConsumerStreamProvider]] with the addition of
 * a datastore.
 */
trait DataStoreConsumerExternalComponents extends KafkaConsumerStreamProvider[String, Message] {

  /**
   * The data store to persist data to.
   */
  val dataStore: DataStore

}

/**
 * A Data Store Consumer is a Kafka Consumer Component that persistently attempts to
 */
trait DataStoreConsumerComponent extends PersistentKafkaConsumerComponent[String, Message] {

  class BalboaConsumer() extends PersistentKafkaConsumer {
    self: DataStoreConsumerExternalComponents with PersistentKafkaConsumerReadiness =>

    /**
     * Attempt to persist a message into a data store.
     *
     * @param message entity to persist.
     * @throws IOException if there was a recoverable error.
     */
    override protected def persist(key: String, message: Message): Unit =
      dataStore.persist(message.getEntityId, message.getTimestamp, message.getMetrics)
  }

}
