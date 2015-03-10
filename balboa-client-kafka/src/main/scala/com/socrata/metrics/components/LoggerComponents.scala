package com.socrata.metrics.components

trait KafkaMetricLoggerComponent extends MetricLoggerComponent {
  type MetricLogger <: MetricLoggerLike


  def MetricLogger(topic: String, backupFileName: String): MetricLogger

  /**
   * Do NOT Use:  Here for legacy reasons.
   *
   * See: [[MetricLoggerComponent]]
   *
   * @param serverName The Messaging Server name
   * @param queueName The name of the queue to place the message.
   * @param backupFileName Where to place the back up files
   * @return MetricLogger
   */
  @Deprecated
  override def MetricLogger(serverName: String, queueName: String, backupFileName: String): MetricLogger = ???
}
