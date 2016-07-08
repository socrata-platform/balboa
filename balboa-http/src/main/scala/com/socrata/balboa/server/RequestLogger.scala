package com.socrata.balboa.server

import com.typesafe.scalalogging.StrictLogging
import org.scalatra.ScalatraServlet

trait RequestLogger extends ScalatraServlet with StrictLogging {
  before() {
    logger info s"in-bound request: ${request.getMethod} ${request.getRequestURI}?${request.getQueryString}"
  }
  after() {
  }
}
