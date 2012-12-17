package com.socrata.balboa.server.rest

import com.socrata.http.server._
import javax.servlet.http.HttpServletRequest
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.socrata.balboa.metrics.data.BalboaFastFailCheck
import com.rojoma.simplearm.util._

/**
 * Version/Health-Check
 */
object VersionRest extends Service[HttpServletRequest, HttpResponse] {
  val versionString = for {
    resource <- managed(getClass.getClassLoader.getResourceAsStream("version"))
    source <- managed(scala.io.Source.fromInputStream(resource, "UTF-8"))
  } yield source.mkString

  def apply(request: HttpServletRequest): HttpResponse = {
    val responseCode = if(BalboaFastFailCheck.getInstance.isInFailureMode) InternalServerError else OK
    responseCode ~> ContentType("application/json") ~> Content(versionString)
  }
}
