package com.socrata.metrics.impl

import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.metrics.{Metric, Metrics}
import com.socrata.metrics.{IdParts, AbstractMetricQueue}
import kafka.message.Message
import kafka.producer.Producer

/**
 * This class mirrors the Event class except that it drops the events in the
 * JMS queue and doesn't actually create "Event" objects, instead it creates
 * messages that the metrics service consumes.
 */
sealed trait MetricKafkaQueue extends AbstractMetricQueue
case class instance(producer: Producer[String, Message]) extends MetricKafkaQueue {

  /**
   * Interface for receiving a Metric.
   *
   * @param entity Entity which this Metric belongs to (ex: a domain).
   * @param name Name of the Metric to store.
   * @param value Numeric value of this metric.
   * @param timestamp Time when this metric was created.
   * @param recordType Type of metric to add, See [[com.socrata.balboa.metrics.Metric.RecordType]] for more information.
   */
  override def create(entity: IdParts, name: IdParts, value: Long, timestamp: Long, recordType: RecordType): Unit = {
    val metrics = new Metrics()
    val metric = new Metric()
    metric.setType(recordType)
    metric.setValue(value)
    metrics.put(name.toString(), metric)
  }

}

object MetricKafkaQueue {

  def instance(): MetricKafkaQueue = {
    MetricKafkaQueue.instance()
  }

}
