package com.socrata.balboa.server.rest

import javax.servlet.http.HttpServletRequest

import com.socrata.balboa.http.BuildInfo
import com.socrata.balboa.metrics.data.BalboaFastFailCheck
import com.socrata.http.server._
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._

/**
 * Version/Health-Check
 */
object VersionRest extends Service[HttpServletRequest, HttpResponse] {
  val versionString = BuildInfo.toJson

  def apply(request: HttpServletRequest): HttpResponse = {
    val responseCode = if(BalboaFastFailCheck.getInstance.isInFailureMode) InternalServerError else OK
    responseCode ~> ContentType("application/json") ~> Content(versionString)
  }
}
