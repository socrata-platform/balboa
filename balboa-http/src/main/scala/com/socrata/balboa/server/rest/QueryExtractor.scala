package com.socrata.balboa.server.rest

import java.net.URLDecoder
import javax.servlet.http.HttpServletRequest

import com.socrata.balboa.common.logging.BalboaLogging

import scala.util.{Failure, Success, Try}

class QueryExtractor(params: Map[String, String]) extends BalboaLogging {

  def this(req: HttpServletRequest) = this(QueryExtractor.breakOut(req.getQueryString))

  /**
   * Attempts to Extract the value from raw input.  What is returned is the [[Try]] instance
   * that encapsulates the extraction job.
   *
   * @param name The Parameter name.
   * @tparam T The type to extract the parameter to.
   * @return The [[Try]] instance representing the extraction.
   */
  def apply[T: Extractable](name: String): Try[T] = params.get(name) match {
      case Some(s) => Extractable[T].extract(s)
      case _ => Failure(new IllegalArgumentException(s"Parameter $name Not Found!"))
  }

  /**
   * Attempts to extract the parameter value and convert it to the correct type.  Gracefully fails and
   * resorts to input backup.
   *
   * @param name The name of the parameter to check.
   * @param orElse The function to use upon failure or missing parameter with "name".
   * @tparam T The type to extract the parameter value to.
   * @return The [[Option]] that represents the existence of the Extracted value.  [[Some]] on success, [[None]] on failure.
   */
  def apply[T: Extractable](name: String, orElse: () => Option[T]): Option[T] = params.get(name) match {
    case Some(s) => Extractable[T].extract(s) match {
      case Success(e) => Some(e)
      case Failure(t) =>
        logger.error(s"Unable to parse $s due to ${t.getMessage}")
        orElse()
    }
    case _ => orElse()
  }
}

object QueryExtractor {
  def breakOut(queryStringOrNull: String): Map[String, String] =
    if(queryStringOrNull == null) Map.empty
    else queryStringOrNull.split('&').map(_.split("=", 2)).collect { case Array(key, value) =>
      URLDecoder.decode(key, "UTF-8") -> URLDecoder.decode(value, "UTF-8")
    }.toMap
}

