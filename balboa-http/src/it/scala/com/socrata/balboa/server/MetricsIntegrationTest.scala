package com.socrata.balboa.server

import java.net.URL

import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.metrics.{Metric, Metrics}
import com.socrata.balboa.metrics.data.{DataStoreFactory}
import com.stackmob.newman.ApacheHttpClient
import com.stackmob.newman.dsl.GET
import com.stackmob.newman.response.HttpResponse
import com.stackmob.newman.response.HttpResponseCode.{BadRequest, NotFound, Ok}
import org.json4s.jackson.JsonMethods.{parse, pretty, render}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import scala.language.implicitConversions

import scala.concurrent.Await
import scala.language.postfixOps

class MetricsIntegrationTest extends FlatSpec with Matchers with BeforeAndAfterEach {
  implicit val httpClient = new ApacheHttpClient
  val dataStore = DataStoreFactory.get()
  val testMetricName = "testMetricsPersisted"
  val testEntityPrefix = "testMetricsEntity"
  var testEntityName = ""

  class AssertionJSON(j: => String) {
    def shouldBeJSON(expected: String) = {
      val actualObj = parse(j)
      val expectObj = parse(expected)

      withClue(
        "\nTextual actual:\n\n" + pretty(render(actualObj)) + "\n\n\n" +
        "Textual expected:\n\n" + pretty(render(expectObj)) + "\n\n")
        { actualObj should be (expectObj) }
    }
  }
  implicit def convertJSONAssertion(j: => String): AssertionJSON = new AssertionJSON(j)

  def getHttpResponse(url: String): HttpResponse = {
    Await.result(GET(new URL(Config.Server, url)).apply, Config.RequestTimeout)
  }

  // Ensures each test interacts with a unique entity
  override def beforeEach() = {
    testEntityName = testEntityPrefix + "-" + java.util.UUID.randomUUID().toString
  }

  "Retrieve /metrics range with no range" should "be fail" in {
    val response = getHttpResponse(s"/metrics/$testEntityName/range")
    response.code.code should be (BadRequest.code)
  }

  "Retrieve /metrics without specifying" should "be not found" in {
    val response = getHttpResponse("/metrics")
    response.code.code should be (NotFound.code)
  }

  "Retrieve /metrics range with a range" should "succeed" in {
    val response = getHttpResponse(s"/metrics/$testEntityName/range?start=2010-01-01+00%3A00%3A00%3A000&end=2017-01-01+00%3A00%3A00%3A000")
    response.code.code should be (Ok.code)
    response.bodyString shouldBeJSON """{}"""
  }

  "Retrieve /metrics/* with no period" should "fail with error msg" in {
    val response = getHttpResponse(s"/metrics/$testEntityName")
    response.code.code should be (BadRequest.code)
    response.bodyString shouldBeJSON """ { "error": 400, "message": "Parameter period required." } """
  }

  "Retrieve /metrics/* with bad period" should "fail with error msg" in {
    val response = getHttpResponse(s"/metrics/$testEntityName?period=crud")
    response.code.code should be (BadRequest.code)
    response.bodyString shouldBeJSON """ { "error": 400, "message": "Unable to parse period : No period named crud" } """
  }

  "Retrieve /metrics/* with no date" should "fail with error msg" in {
    val response = getHttpResponse(s"/metrics/$testEntityName?period=YEARLY")
    response.code.code should be (BadRequest.code)
    response.bodyString shouldBeJSON """ { "error": 400, "message": "Parameter date required." } """
  }

  "Retrieve /metrics/* with bad date" should "fail with error msg" in {
    val response = getHttpResponse(s"/metrics/$testEntityName?period=YEARLY&date=crud")
    response.code.code should be (BadRequest.code)
    response.bodyString shouldBeJSON """ { "error": 400, "message": "Unable to parse date crud" } """
  }

  "Retrieve /metrics/* with valid period and date" should "succeed" in {
    val response = getHttpResponse(s"/metrics/$testEntityName?period=YEARLY&date=1960-01-01")
    response.code.code should be (Ok.code)
    response.bodyString shouldBeJSON """{}"""
  }

  // Returns the JSON string representation of the metric added
  def persistSingleMetric(): String = {
    val metrics = new Metrics()
    metrics.put(testMetricName, new Metric(RecordType.ABSOLUTE, 1))
    dataStore.persist(testEntityName, 0, metrics)

    """{ "testMetricsPersisted": { "value": 1, "type": "absolute" } }"""
  }

  // Returns the JSON string representation of the metrics added
  def persistManyMetrics(metRange: Range): String = {
    val metrics = new Metrics()
    for (i <- metRange) {
      metrics.put(testMetricName + i, new Metric(RecordType.ABSOLUTE, 1))
    }
    dataStore.persist(testEntityName, 0, metrics)

    "{" +
      metRange.map(i =>
        """ "testMetricsPersisted%d": { "value": 1, "type": "absolute" } """
          .format(i))
          .mkString(", ") +
      "}"
  }

  "Retrieve /metrics range after persisting" should "show persisted metrics" in {
    val expected = persistSingleMetric()
    val response = getHttpResponse(s"metrics/$testEntityName/range?start=1969-01-01&end=1970-02-02")
    response.code.code should be (Ok.code)
    response.bodyString shouldBeJSON expected
  }

  "Retrieve /metrics range after persisting multiple metrics" should "show multiple persisted metrics" in {
    val metRange = 1 to 10
    val expected = persistManyMetrics(metRange)
    val response = getHttpResponse(s"metrics/$testEntityName/range?start=1969-01-01&end=1970-02-02")
    response.code.code should be (Ok.code)

    response.bodyString shouldBeJSON expected
  }

  "Retrieve /metrics series after persisting" should "show persisted metrics" in {
    val expectedMetric = persistSingleMetric()

    val response = getHttpResponse(s"metrics/$testEntityName/series?period=MONTHLY&start=1969-12-01&end=1970-02-02")
    response.code.code should be (Ok.code)

    val expectSeries1 = """ { "start" : -2678400000, "end" : -1, "metrics" : { } } """
    val expectSeries2 = """ { "start" : 0, "end" : 2678399999, "metrics" : %s } """.format(expectedMetric)
    val expectSeries3 = """ { "start" : 2678400000, "end": 5097599999, "metrics" : { } } """
    val expected = s"[ $expectSeries1, $expectSeries2, $expectSeries3 ]"
    response.bodyString shouldBeJSON expected
  }

  "Retrieve /metrics series after persisting multiple metrics" should "show persisted metrics" in {
    val metRange = 1 to 10
    val expectedMetrics = persistManyMetrics(metRange)

    val response = getHttpResponse(s"metrics/$testEntityName/series?period=MONTHLY&start=1969-12-01&end=1970-02-02")
    response.code.code should be (Ok.code)

    val expectSeries1 = """ { "start" : -2678400000, "end" : -1, "metrics" : { } } """
    val expectSeries2 = """ { "start" : 0, "end" : 2678399999, "metrics" : %s } """.format(expectedMetrics)
    val expectSeries3 = """ { "start" : 2678400000, "end": 5097599999, "metrics" : { } } """
    val expected = s"[ $expectSeries1, $expectSeries2, $expectSeries3 ]"
    response.bodyString shouldBeJSON expected
  }
}
