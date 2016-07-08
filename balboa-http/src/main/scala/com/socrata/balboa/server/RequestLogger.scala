package com.socrata.balboa.server

import com.typesafe.scalalogging.StrictLogging
import org.scalatra.ScalatraServlet

trait RequestLogger extends ScalatraServlet with StrictLogging {
  before() {
    val requestParams = Option(request.getQueryString).map("?" + _).getOrElse("")
    logger info s"in-bound request: ${request.getMethod} ${request.getRequestURI}$requestParams"
  }
}
