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
  def apply(request: HttpServletRequest): HttpResponse = {
    val inFastFail = BalboaFastFailCheck.getInstance().isInFailureMode
    val versionString = for {
      resource <- managed(getClass.getClassLoader.getResourceAsStream("version"))
      source <- managed(scala.io.Source.fromInputStream(resource, "UTF-8"))
    } yield source.mkString

    if (inFastFail)
      InternalServerError ~> Content(versionString)
    else
      OK ~> Content(versionString)

  }
}
