package com.socrata.balboa.server

import javax.servlet.http.HttpServletRequest

import com.socrata.balboa.BuildInfo
import com.socrata.balboa.metrics.data.BalboaFastFailCheck
import com.socrata.balboa.server.rest.{EntitiesRest, MetricsRest}
import com.socrata.balboa.server.ResponseWithType.json
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.{InternalServerError, NotFound, Ok, ScalatraServlet}
import org.scalatra.json.JacksonJsonSupport

import scala.collection.JavaConverters._

class MainServlet extends ScalatraServlet
    with StrictLogging
    with JacksonJsonSupport
    with UnexpectedErrorFilter {

  override protected implicit val jsonFormats: Formats = DefaultFormats

  val versionString = BuildInfo.toJson

  def getAccepts(req: HttpServletRequest): Seq[String] = {
    req.getHeaders("accept").asScala.toSeq
  }

  get("/*") {
    contentType = json
    NotFound(Error(404, "Not found."))
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

  // Match paths like /metrics/:entityId and /metrics/:entityId/whatever
  get("""^\/metrics\/([^\/]*).*""".r) {
    val entityId = params("captures")
    val response = MetricsRest.get(entityId, params, getAccepts(request))
    contentType = response.contentType
    response.result
  }

  post("/metrics/:entityId") {
    val entityId = params("entityId")
    val entityOpt = parsedBody.extractOpt[EntityJSON]
    val response = MetricsRest.post(entityId, entityOpt)
    contentType = response.contentType
    response.result
  }

  get("/metrics/:entityId/range*") {
    val entityId = params("entityId")
    val response = MetricsRest.range(entityId, params, getAccepts(request))
    contentType = response.contentType
    response.result
  }

  get("/metrics/:entityId/series*") {
    val entityId = params("entityId")
    val response = MetricsRest.series(entityId, params, getAccepts(request))
    contentType = response.contentType
    response.result
  }
}
