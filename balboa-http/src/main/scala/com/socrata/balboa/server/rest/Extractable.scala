package com.socrata.balboa.server.rest

import com.socrata.balboa.metrics.data.Period

trait Extractable[T] {
  def extract(raw: String): Either[String, T]
}

object Extractable {
  def apply[T](implicit ex: Extractable[T]): Extractable[T] = ex

  implicit object ExtractString extends Extractable[String] {
    def extract(raw: String): Right[Nothing, String] = Right(raw)
  }

  implicit object ExtractInt extends Extractable[Int] {
    def extract(raw: String): Either[String, Int] = try {
      Right(raw.toInt)
    } catch {
      case e: NumberFormatException => Left(e.getMessage)
    }
  }

  implicit object extractDateRangePeriod extends Extractable[Period] {
    def extract(raw: String): Either[String, Period] = try {
      Right(Period.valueOf(raw.toUpperCase))
    } catch {
      case e: IllegalArgumentException => Left("No period named " + raw)
    }
  }
}
