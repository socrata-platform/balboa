package com.socrata.balboa.server

import com.socrata.balboa.server.ResponseWithType.json
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.eclipse.jetty.http.HttpStatus.INTERNAL_SERVER_ERROR_500
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
      contentType = json
      InternalServerError(Error(INTERNAL_SERVER_ERROR_500, "Server error: " + e.getMessage))
  }
}
