package com.socrata.balboa.server.util

import java.text.SimpleDateFormat
import java.util.Date

import scala.util.{Failure, Success, Try}

/**
 * Scala Helper class to assist in parsing Dates
 */
object DateParser {

  val dateTimeParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")
  val dateParser = new SimpleDateFormat("yyyy-MM-dd")

  /**
   * Attempt to parse the input String into the acceptable [[Date]] formats.
   *
   * @param dateString The String representation of date time.
   * @return The [[Try]] object that attempts to parse.
   */
  def parse(dateString: String): Try[Date] = Try(dateTimeParser.parse(dateString)) match {
    case Success(d) => Success(d)
    case Failure(ex) => Try(dateParser.parse(dateString))
  }

}
