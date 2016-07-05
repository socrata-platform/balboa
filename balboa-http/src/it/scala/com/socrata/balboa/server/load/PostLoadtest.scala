package com.socrata.balboa.server.load

import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.server.{Config, EntityJSON, MetricJSON, ServiceUtils}
import io.gatling.core.Predef._
import io.gatling.http.Predef._
import org.json4s.{DefaultFormats, Extraction, Formats}
import org.json4s.jackson.JsonMethods.{pretty, render}

import scala.concurrent.duration._

class PostLoadTest extends Simulation {
  protected implicit val jsonFormats: Formats = DefaultFormats

  val httpConf = http.baseURL(Config.Server.toString)

  val testEntityId = "loadTestEntity"
  val testPostUrl = s"metrics/$testEntityId"

  val testLoadVal = 57
  val testLoadType = RecordType.ABSOLUTE.toString.toUpperCase
  val testLoadDate = "1900-01-01"
  val testLoadEpoch = ServiceUtils.parseDate(testLoadDate).get.getTime
  val testLoadEntity = pretty(render(Extraction.decompose(EntityJSON(testLoadEpoch,
    Map("loadTestMetric" -> MetricJSON(testLoadVal, testLoadType))))))

  val serialInsertPopulationBuilder = scenario("Serial insert")
    .repeat(100) {
        exec(http("insert metric")
          .post(testPostUrl)
          .body(StringBody(testLoadEntity)))
    }
    .inject(rampUsers(100) over 10.seconds)

  setUp(
    serialInsertPopulationBuilder
  ).protocols(httpConf).maxDuration(30.seconds)
}
