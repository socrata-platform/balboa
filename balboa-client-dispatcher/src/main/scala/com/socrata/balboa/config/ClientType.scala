package com.socrata.balboa.config

/**
 *
 */
object ClientType extends Enumeration {
  type ClientType = Value
  val jms, kafka /*Add more Client Types here*/ = Value
}
