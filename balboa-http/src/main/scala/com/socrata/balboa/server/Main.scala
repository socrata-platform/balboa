package com.socrata.balboa.server

import com.socrata.http.server.SocrataServerJetty
import javax.servlet.http.HttpServletRequest
import com.socrata.balboa.metrics.config.Configuration
import com.socrata.balboa.server.rest.{MetricsRest, EntitiesRest}
import org.apache.log4j.PropertyConfigurator
import com.socrata.http.routing.{HttpMethods, SimpleRoute, SimpleRouter}
import com.socrata.http.server.{Service, HttpResponse, SimpleFilter}
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.socrata.util.logging.LazyStringLogger

class Main

object Main extends App {
  // set up log4j
  PropertyConfigurator.configure(Configuration.get)

  val log = LazyStringLogger[Main]

  type BalboaService = Service[HttpServletRequest, HttpResponse]

  val router = new SimpleRouter[BalboaService](
    new SimpleRoute(Set(HttpMethods.GET), "entities") -> EntitiesRest,
    new SimpleRoute(Set(HttpMethods.GET), "metrics", ".*".r, "range") -> MetricsRest.range,
    new SimpleRoute(Set(HttpMethods.GET), "metrics", ".*".r, "series") -> MetricsRest.series,
    new SimpleRoute(Set(HttpMethods.GET), "metrics", ".*".r) -> MetricsRest.get
  )

  def logger = new SimpleFilter[HttpServletRequest, HttpResponse] {
    def apply(req: HttpServletRequest, serv: BalboaService) = {
      log.info("Server in-bound request: " + req.getMethod + " " + req.getRequestURI + Option(req.getQueryString).map("?" + _).getOrElse(""))
      serv(req)
    }
  }

  def service(req: HttpServletRequest): HttpResponse =
    router(req.getMethod, req.getRequestURI.split('.').head.split('/').tail) match {
      case Some(s) =>
        s(req)
      case None =>
        NotFound ~> ContentType("application/json") ~> Content("{\"error\": 404, \"message\": \"Not found.\"}")
    }

  val server = new SocrataServerJetty(logger andThen service, port = 9898)
  server.run()
}
