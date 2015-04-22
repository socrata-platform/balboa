package com.socrata.balboa.server.rest

import java.net.URLDecoder
import javax.servlet.http.HttpServletRequest

class QueryExtractor(params: Map[String, String]) {
  def this(req: HttpServletRequest) = this(QueryExtractor.breakOut(req.getQueryString))

  def apply[T: Extractable](name: String): Option[Either[String, T]] = {
    params.get(name).map { rawValue =>
      Extractable[T].extract(rawValue)
    }
  }

  def apply[T: Extractable](name: String, orElse: => T): Either[String, T] = {
    params.get(name).map { rawValue =>
      Extractable[T].extract(rawValue)
    }.getOrElse(Right(orElse))
  }
}

object QueryExtractor {
  def breakOut(queryStringOrNull: String): Map[String, String] =
    if(queryStringOrNull == null) Map.empty
    else queryStringOrNull.split('&').map(_.split("=", 2)).collect { case Array(key, value) =>
      URLDecoder.decode(key, "UTF-8") -> URLDecoder.decode(value, "UTF-8")
    }.toMap
}
