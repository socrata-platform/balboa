import java.util.Date

import com.socrata.balboa.metrics.{EntityJSON, Metric, MetricJSON}
import com.socrata.metrics.{IdParts, MetricQueue}

class HttpMetricQueue extends MetricQueue {

  def HttpMetricQueue() = {}

  /**
   * Interface for receiving a Metric
   *
   * @param entity Entity which this Metric belongs to (ex: a domain).
   * @param name Name of the Metric to store.
   * @param value Numeric value of this metric.
   * @param timestamp Time when this metric was created.
   * @param recordType Type of metric to add, See [[Metric.RecordType]] for more information.
   */
  override def create(entity: IdParts,
             name: IdParts,
             value: Long,
             timestamp: Long = new Date().getTime,
             recordType: Metric.RecordType = Metric.RecordType.AGGREGATE): Unit = {

    // TODO: lazy load metrics for the same entity and send them over in batches
    val metricToWrite = EntityJSON(timestamp, Map(name.toString -> MetricJSON(value, recordType.toString)))

    // TODO: Send to balboa-http
  }

  override def close(): Unit = {}
}
