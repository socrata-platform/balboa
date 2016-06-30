package com.socrata.balboa.metrics

case class EntityJSON(timestamp: Long, metrics: Map[String, MetricJSON])

case class MetricJSON(value: Long, `type`: String)
