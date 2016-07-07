package com.socrata.balboa.server.load

import com.socrata.balboa.server.Config
import io.gatling.core.Predef._
import io.gatling.core.structure.ScenarioBuilder
import io.gatling.http.Predef._

import scala.concurrent.duration._
/**
  * Load test simulating a heavy load of repeated GET requests to
  * the metrics/:entityId endpoint of balboa-http
  */
class QueryLoadTest extends Simulation {

  val httpConf = http.baseURL(Config.Server.toString)

  val testEntityId = "loadTestQueryEntity"
  val testPeriod = "YEARLY"
  val testDate = "1970-01-01"
  val testGetUrl = s"metrics/$testEntityId?period=$testPeriod&date=$testDate"

  val scn: ScenarioBuilder =
    scenario(s"Repeated serial querying at /metrics/$testEntityId with many users")
        .during(30.seconds) {
          exec(http(s"query metric")
            .get(testGetUrl))
        }

  setUp(
    scn.inject(
      nothingFor(1.second),
      atOnceUsers(1),
      nothingFor(5.seconds),
      rampUsers(3000) over 20.seconds
    )
  ).protocols(httpConf).maxDuration(40.seconds)
}
