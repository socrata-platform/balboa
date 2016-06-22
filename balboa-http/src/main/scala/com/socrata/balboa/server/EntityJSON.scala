package com.socrata.balboa.server

case class EntityJSON(timestamp: Long, metrics: Map[String, MetricJSON])

case class MetricJSON(value: Int, `type`: String)
