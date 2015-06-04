package com.socrata.balboa.server.rest

import java.util.Date

import com.socrata.balboa.metrics.data.Period
import com.socrata.balboa.server.util.DateParser

import scala.util.{Success, Try}

/**
 * Trait that defines the interface on how to extract a type out of a Raw String.
 *
 * @tparam T The type to convert to.
 */
trait Extractable[T] {

  /*
  - TODO Possible to just replace with traditional Scala Implicit Conversions
  - TODO Replace Either with Scala Try.
   */

  def extract(raw: String): Try[T]
}

object Extractable {
  def apply[T](implicit ex: Extractable[T]) = ex

  implicit object ExtractString extends Extractable[String] {
    override def extract(raw: String) = Success(raw)
  }

  implicit object ExtractInt extends Extractable[Int] {
    override def extract(raw: String) = Try(raw.toInt)
  }

  implicit object extractDateRangePeriod extends Extractable[Period] {
    override def extract(raw: String) = Try(Period.valueOf(raw.toUpperCase))
  }

  implicit object ExtractBoolean extends Extractable[Boolean] {
    override def extract(raw: String) = Try(raw.toBoolean)
  }

  implicit object ExtractDate extends Extractable[Date] {
    override def extract(raw: String) = DateParser.parse(raw)
  }

}
