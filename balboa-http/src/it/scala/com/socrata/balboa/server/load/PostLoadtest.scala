package com.socrata.balboa.server.load

import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.server.{Config, EntityJSON, MetricJSON, ServiceUtils}
import io.gatling.core.Predef._
import io.gatling.http.Predef._
import org.json4s.{DefaultFormats, Extraction, Formats}
import org.json4s.jackson.JsonMethods.{pretty, render}

class PostLoadTest extends Simulation {
  protected implicit val jsonFormats: Formats = DefaultFormats

  val httpConf = http.baseURL(Config.Server.toString)

  val testLoadVal = 57
  val testLoadType = RecordType.ABSOLUTE.toString.toUpperCase
  val testLoadDate = "1900-01-01"
  val testLoadEpoch = ServiceUtils.parseDate(testLoadDate).get.getTime
  val testLoadEntity = pretty(render(Extraction.decompose(EntityJSON(testLoadEpoch,
    Map("loadTestMetric" -> MetricJSON(testLoadVal, testLoadType))))))

  val serialInsertPopulationBuilder = scenario("Serial insert")
    .exec(http("insert metric").post("/metrics/loadEntity").body(StringBody(testLoadEntity)))
    .inject(atOnceUsers(1))

  setUp(
    serialInsertPopulationBuilder
  ).protocols(httpConf)
}
