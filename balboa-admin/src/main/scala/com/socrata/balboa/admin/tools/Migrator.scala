package com.socrata.balboa.admin.tools

import java.util.Date

import com.socrata.balboa.metrics.data.Period

/**
  * Migrates metrics from one entity id to the other.
  *
  * Created by michaelhotan on 11/2/15.
  */
object Migrator {

  def migrate(source: String,
              destination: String,
              start: Option[Date] = None,
              end: Option[Date] = None,
              granularity: Option[Period] = None): Unit = {
    val startDate = start match {
      case Some(d) => d
      case None => new Date(0)
    }
    val endDate = end match {
      case Some(d) => d
      case None => new Date()
    }

//    val d: Dumper = new Dumper()
    granularity match {
      case Some(g) =>

      case None =>

    }
  }

}
