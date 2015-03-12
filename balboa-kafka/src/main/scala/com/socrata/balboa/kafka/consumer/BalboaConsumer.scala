package com.socrata.balboa.kafka.consumer

import com.socrata.balboa.kafka.codec.BalboaMessageCodecLike
import com.socrata.balboa.metrics.Message
import com.socrata.balboa.metrics.data.DataStore
import kafka.consumer.KafkaStream

/**
 * Consumer that ingest metrics and funnels them into an underlying data store.
 */
case class BalboaConsumer(stream: KafkaStream[Array[Byte],Array[Byte]],
                          waitTime: Long,
                          ds: DataStore) extends PersistentConsumer[Message](stream, waitTime) with BalboaMessageCodecLike {

  /**
   * @param m Metrics message to persist to the data store
   */
  override protected def persist(m: Message): Unit = ds.persist(m.getEntityId, m.getTimestamp, m.getMetrics)

}
