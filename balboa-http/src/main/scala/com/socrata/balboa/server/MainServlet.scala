package com.socrata.balboa.server

import javax.servlet.http.HttpServletRequest

import com.socrata.balboa.BuildInfo
import com.socrata.balboa.metrics.data.BalboaFastFailCheck
import com.socrata.balboa.server.rest.EntitiesRest
import com.socrata.balboa.server.ResponseWithType.json
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.{InternalServerError, Ok, ScalatraServlet}
import org.scalatra.json.JacksonJsonSupport

import scala.collection.JavaConverters._

class MainServlet extends ScalatraServlet
    with ClientCounter
    with StrictLogging
    with JacksonJsonSupport
    with NotFoundFilter
    with UnexpectedErrorFilter {

  override protected implicit val jsonFormats: Formats = DefaultFormats

  val versionString = BuildInfo.toJson

  def getAccepts(req: HttpServletRequest): Seq[String] = {
    req.getHeaders("accept").asScala.toSeq
  }

  get("/version*") {
    contentType = json
    if (BalboaFastFailCheck.getInstance.isInFailureMode) {
      InternalServerError
    } else {
      Ok(versionString)
    }
  }

  get("/entities*") {
    val response = EntitiesRest(params)
    contentType = response.contentType
    response.result
  }
}
