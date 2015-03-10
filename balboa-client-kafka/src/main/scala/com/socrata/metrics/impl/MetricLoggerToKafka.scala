package com.socrata.metrics.impl

import java.net.{Inet4Address, InetAddress}

import com.socrata.metrics.components.MetricLoggerComponent

/**
 * @param address See: [[InetAddress]]
 * @param port Integer representation of port. IE. 8080, 9062
 */
case class AddressAndPort(address: InetAddress, port: Int) {
  override def toString: String = address.getHostAddress + ":" + port
}

/**
 * TODO:
 */
trait MetricLoggerToKafka extends MetricLoggerComponent {
  type MetricLogger <: MetricLoggerLike

  def MetricLogger(topicName)

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
