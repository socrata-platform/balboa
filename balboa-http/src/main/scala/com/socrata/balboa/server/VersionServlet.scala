package com.socrata.balboa.server

import com.socrata.balboa.BuildInfo
import com.socrata.balboa.metrics.data.BalboaFastFailCheck
import com.socrata.balboa.server.ResponseWithType.json
import org.scalatra.{InternalServerError, Ok, ScalatraServlet}

class VersionServlet extends ScalatraServlet with NotFoundFilter {

  val versionString = BuildInfo.toJson

  get("*") {
    contentType = json
    if (BalboaFastFailCheck.getInstance.isInFailureMode) {
      InternalServerError
    } else {
      Ok(versionString)
    }
  }
}
