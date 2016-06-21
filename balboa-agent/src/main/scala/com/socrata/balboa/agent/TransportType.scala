package com.socrata.balboa.agent

sealed trait TransportType
object Http extends TransportType
object Mq extends TransportType
