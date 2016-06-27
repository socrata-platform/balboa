package com.socrata.balboa.server

import org.scalatra.{NotFound, ScalatraServlet}
import org.eclipse.jetty.http.HttpStatus.NOT_FOUND_404
import com.socrata.balboa.server.ResponseWithType.json

trait NotFoundFilter extends ScalatraServlet
  with JacksonJsonServlet {

  get("/*") {
    contentType = json
    NotFound(Error(NOT_FOUND_404, "Not found."))
  }

}
