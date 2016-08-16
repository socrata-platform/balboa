package com.socrata.balboa.metrics.data.impl

import com.socrata.balboa.metrics.data.Period
import com.typesafe.config.Config

import scala.collection.JavaConverters._

object SupportedPeriods {
  def supportedPeriods(conf: Config): List[Period] =
    conf.getList("balboa.summaries").unwrapped()
      .asScala.map(_.asInstanceOf[String].toUpperCase).map(Period.valueOf).toList

  def getSupportedPeriodsJava(conf: Config): java.util.List[Period] = supportedPeriods(conf).asJava
}
