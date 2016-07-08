package com.socrata.balboa.server.load

import com.socrata.balboa.server.Config
import io.gatling.core.Predef._
import io.gatling.core.structure.ScenarioBuilder
import io.gatling.http.Predef._

import scala.concurrent.duration._

/**
  * Load test simulating a heavy load of repeated GET requests to
  * the metrics/:entityId/range endpoint of balboa-http
  */
class RangeQueryLoadTest extends Simulation {

  val httpConf = http.baseURL(Config.Server.toString)

  val testEntityId = "loadTestRangeQueryEntity"
  val testPeriod = "YEARLY"
  val testStartDate = "1970-01-01"
  val testEndDate = "1970-01-02"
  val testGetUrl = s"metrics/$testEntityId/range?period=$testPeriod&start=$testStartDate&end=$testEndDate"

  val scn: ScenarioBuilder =
    scenario(s"Repeated serial range querying at /metrics/$testEntityId with many users")
        .during(30.seconds) {
          exec(http(s"query metric range")
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
