package com.socrata.balboa.server

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.scalatra.{InternalServerError, ScalatraServlet}
import org.scalatra.json.JacksonJsonSupport

trait UnexpectedErrorFilter extends ScalatraServlet
  with StrictLogging
  with JacksonJsonSupport {

  // Generic error handling so that all unexpected errors will result in logging
  // a stack trace and returning a 500 status code.
  error {
    case e: Throwable =>
      logger.error("Fatal error", e)
      InternalServerError(Error(500, "Server error: " + e.getMessage))
  }
}
