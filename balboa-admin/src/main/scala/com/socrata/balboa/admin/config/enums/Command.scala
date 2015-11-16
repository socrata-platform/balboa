package com.socrata.balboa.admin.config.enums

/**
  * Created by michaelhotan on 11/2/15.
  */
object Command extends Enumeration {
  type Command = Value
  val none, emit, migrate = Value
}
