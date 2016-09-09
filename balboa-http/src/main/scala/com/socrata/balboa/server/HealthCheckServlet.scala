package com.socrata.balboa.server

import javax.servlet.http.HttpServletResponse

import com.socrata.balboa.BuildInfo
import com.socrata.balboa.metrics.data.{DataStoreFactory, DefaultDataStoreFactory}
import org.scalatra.ScalatraServlet

import scala.util.{Failure, Success, Try}

class HealthCheckServletWithDefaultDataStore extends HealthCheckServlet(DefaultDataStoreFactory)

class HealthCheckServlet(dataStoreFactory: DataStoreFactory) extends ScalatraServlet
  with NotFoundFilter
  with RequestLogger {

  lazy val dataStore = dataStoreFactory.get

  get("/") {
    val cassandra = Try(dataStore.checkHealth()) match {
      case Success(_) => 1
      case Failure(_) => 0
    }

    val serving = if (ClientCounter.get() > 0) { 1 } else { 0 }

    contentType = "text/plain"

    status = if (cassandra == 1) {
      HttpServletResponse.SC_OK
    } else {
      HttpServletResponse.SC_SERVICE_UNAVAILABLE
    }

    s"""healthy: $cassandra
      |healthy.cassandra: $cassandra
      |serving: $serving
      |version: ${BuildInfo.version}
      |revision: ${BuildInfo.revision}
    """.stripMargin
  }
}
