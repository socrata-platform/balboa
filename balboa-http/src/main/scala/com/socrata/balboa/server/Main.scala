package com.socrata.balboa.server

import javax.servlet.http.HttpServletRequest

import com.socrata.balboa.BuildInfo
import com.socrata.balboa.metrics.data.BalboaFastFailCheck
import com.socrata.balboa.server.rest.{EntitiesRest, MetricsRest}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.eclipse.jetty.server.Server
import org.scalatra.{InternalServerError, NotFound, Ok, ScalatraServlet}
import org.slf4j.LoggerFactory
import org.eclipse.jetty.webapp.WebAppContext
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.servlet.ScalatraListener
import org.scalatra.json.JacksonJsonSupport

import scala.collection.JavaConverters._

class MainServlet extends ScalatraServlet with StrictLogging with JacksonJsonSupport {

  protected implicit val jsonFormats: Formats = DefaultFormats

  val versionString = BuildInfo.toJson

  def getAccepts(req: HttpServletRequest): Seq[String] = {
    req.getHeaders("accept").asScala.toSeq
  }

  get("/*") {
    contentType = "application/json"
    NotFound(Error(404, "Not found."))
  }
  get("/version*") {
    contentType = "application/json"
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
  get("/metrics/:entityId") {
    val entityId = params("entityId")
    val response = MetricsRest.get(entityId, params, getAccepts(request))
    contentType = response.contentType
    response.result
  }
  // Catch the extra paths to match behavior of old API
  get("/metrics/:entityId/*") {
    val entityId = params("entityId")
    val response = MetricsRest.get(entityId, params, getAccepts(request))
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


object Main extends App {
  val logger = LoggerFactory.getLogger(getClass)

  val DefaultPort = 9012
  val port = if (args.length > 0) {
    try {
      args(0).toInt
    } catch {
      case e: NumberFormatException =>
        logger.error("Expected the argument '{}' to be a port number.", args(0))
        sys.exit(1)
    }
  } else {
    DefaultPort
  }

  val server = new Server(port)
  val context = new WebAppContext()

  context.setContextPath("/")
  context.setResourceBase("src/main/webapp")
  context.addEventListener(new ScalatraListener())
  context.addServlet(classOf[MainServlet], "/")

  server.setHandler(context)

  logger.info("Starting balboa-http service on port '{}'.", port.toString)
  server.start()
  server.join()
}
