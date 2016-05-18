package com.socrata.balboa.server

import javax.servlet.http.HttpServletRequest

import com.socrata.balboa.metrics.config.Configuration
import com.socrata.balboa.server.rest.{EntitiesRest, MetricsRest, VersionRest}
import com.socrata.http.routing.{HttpMethods, SimpleRoute, SimpleRouter}
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.{HttpResponse, Service, SimpleFilter, SocrataServerJetty}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.log4j.PropertyConfigurator

class Main

object Main extends App with StrictLogging {
  val DefaultPort = 9012

  PropertyConfigurator.configure(Configuration.get)

  type BalboaService = Service[HttpServletRequest, HttpResponse]

  val router = new SimpleRouter[BalboaService](
    new SimpleRoute(Set(HttpMethods.GET), "version") -> VersionRest,
    new SimpleRoute(Set(HttpMethods.GET), "entities") -> EntitiesRest,
    new SimpleRoute(Set(HttpMethods.GET), "metrics", ".*".r, "range") -> MetricsRest.range,
    new SimpleRoute(Set(HttpMethods.GET), "metrics", ".*".r, "series") -> MetricsRest.series,
    new SimpleRoute(Set(HttpMethods.GET), "metrics", ".*".r) -> MetricsRest.get
  )

  def requestLoggingFilter = new SimpleFilter[HttpServletRequest, HttpResponse] {
    def apply(req: HttpServletRequest, serv: BalboaService) = {
      logger.info("Server in-bound request: " + req.getMethod + " " + req.getRequestURI + Option(req.getQueryString).map("?" + _).getOrElse(""))
      serv(req)
    }
  }

  def service(req: HttpServletRequest): HttpResponse =
    router(req.getMethod, req.getRequestURI.split('/').tail) match {
      case Some(s) =>
        s(req)
      case None    =>
        NotFound ~> ContentType("application/json") ~> Content("{\"error\": 404, \"message\": \"Not found.\"}")
    }

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

  logger.info("Starting balboa-http service on port '{}'.", port.toString)
  val server = new SocrataServerJetty(requestLoggingFilter andThen service, port = port)
  server.run()
}
