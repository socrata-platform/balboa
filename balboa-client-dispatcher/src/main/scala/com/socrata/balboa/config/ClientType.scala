package com.socrata.balboa.config

/**
 * Specific type of Balboa Client to use to emit Metrics Messages.
 */
object ClientType extends Enumeration {
  type ClientType = Value
  val jms, kafka /* Add more Client Types here */ = Value
}
