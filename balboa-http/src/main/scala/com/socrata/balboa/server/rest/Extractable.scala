package com.socrata.balboa.server.rest

import com.socrata.balboa.metrics.data.Period

trait Extractable[T] {

  /*
  - TODO Replace Either with Try
   */

  def extract(raw: String): Either[String, T]
}

object Extractable {
  def apply[T](implicit ex: Extractable[T]) = ex

  implicit object ExtractString extends Extractable[String] {
    def extract(raw: String) = Right(raw)
  }

  implicit object ExtractInt extends Extractable[Int] {
    def extract(raw: String) = try {
      Right(raw.toInt)
    } catch {
      case e: NumberFormatException => Left(e.getMessage)
    }
  }

  implicit object extractDateRangePeriod extends Extractable[Period] {
    def extract(raw: String) = try {
      Right(Period.valueOf(raw.toUpperCase))
    } catch {
      case e: IllegalArgumentException => Left("No period named " + raw)
    }
  }

  implicit object ExtractBoolean extends Extractable[Boolean] {
    override def extract(raw: String): Either[String, Boolean] = try {
      Right(raw.toBoolean)
    } catch {
      case e: IllegalArgumentException => Left(e.getMessage)
    }
  }
}
