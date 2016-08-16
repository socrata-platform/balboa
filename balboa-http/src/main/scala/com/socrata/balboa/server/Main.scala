package com.socrata.balboa.server

import com.socrata.balboa.util.LoggingConfigurator
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.webapp.WebAppContext
import org.scalatra.servlet.ScalatraListener

object Main extends App with StrictLogging {
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

  LoggingConfigurator.configureLogging(ConfigFactory.load())

  val server = new Server(port)
  val context = new WebAppContext()

  context.setContextPath("/")
  context.setResourceBase("src/main/webapp")
  context.addEventListener(new ScalatraListener())
  context.addServlet(classOf[VersionServlet], "/version")
  context.addServlet(classOf[EntitiesServletWithDefaultDataStore], "/entities")
  context.addServlet(classOf[MetricsServletWithDefaultDatastore], "/metrics")
  context.addServlet(classOf[HealthCheckServletWithDefaultDataStore], "/health")
  context.addServlet(classOf[NotFoundServlet], "/")

  server.setHandler(context)

  logger.info("Starting balboa-http service on port '{}'.", port.toString)
  server.start()
  server.join()
}
