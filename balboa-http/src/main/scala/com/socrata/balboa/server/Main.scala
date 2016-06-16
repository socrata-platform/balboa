package com.socrata.balboa.server

import com.socrata.balboa.server.rest.{EntitiesRest, MetricsRest, VersionRest}
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.eclipse.jetty.server.Server
import org.scalatra.ScalatraServlet
import org.slf4j.LoggerFactory
import org.eclipse.jetty.webapp.WebAppContext
import org.scalatra.servlet.ScalatraListener

class MainServlet extends ScalatraServlet with StrictLogging {
  get("/*") {
    (NotFound ~> ContentType("application/json") ~> Content("{\"error\": 404, \"message\": \"Not found.\"}"))(response)
  }
  get("/version*") {
    VersionRest(request)(response)
  }
  get("/entities*") {
    EntitiesRest(request)(response)
  }
  get("/metrics/*") {
    MetricsRest.get(request)(response)
  }
  get("/metrics/*/range") {
    MetricsRest.range(request)(response)
  }
  get("/metrics/*/series") {
    MetricsRest.series(request)(response)
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
