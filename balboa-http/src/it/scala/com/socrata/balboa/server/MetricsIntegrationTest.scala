package com.socrata.balboa.server

import java.net.URL

import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.metrics.{Metric, Metrics}
import com.socrata.balboa.metrics.data.DataStoreFactory
import com.stackmob.newman.ApacheHttpClient
import com.stackmob.newman.dsl.GET
import com.stackmob.newman.response.{HttpResponse, HttpResponseCode}
import com.stackmob.newman.response.HttpResponseCode.{BadRequest, NotFound, Ok}
import org.json4s.jackson.JsonMethods.{parse, pretty, render}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

import scala.language.implicitConversions
import scala.concurrent.Await
import scala.language.postfixOps

class MetricsIntegrationTest extends FlatSpec with Matchers with BeforeAndAfterEach {
  implicit val httpClient = new ApacheHttpClient
  val dataStore = DataStoreFactory.get()
  val testMetricPrefix = "testMetric"
  val testEntityPrefix = "testMetricsEntity"
  var testEntityName = ""
  var testMetricName = ""
  val testStart = "1969-12-01"
  val testPersistedDate = "1970-01-01"
  val testPersistedDateEpoch = ServiceUtils.parseDate(testPersistedDate).get.getTime
  val testEnd = "1970-02-02"
  val protobuf = "application/x-protobuf"

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

  case class JSONAndProtoResponse(json: HttpResponse, proto: HttpResponse) {
    def shouldHaveCode(code: HttpResponseCode) = {
      json.code.code should be (code.code)
      proto.code.code should be (code.code)
    }
  }

  def getJSONResponse(url: String): HttpResponse = {
    Await.result(GET(new URL(Config.Server, url)).apply, Config.RequestTimeout)
  }

  def getJSONProtoResponse(url: String): JSONAndProtoResponse = {
    JSONAndProtoResponse (
      getJSONResponse(url),
      Await.result(GET(new URL(Config.Server, url)).setHeaders(("Accept", protobuf)).apply, Config.RequestTimeout)
    )
  }

  // Ensures each test interacts with a unique entity
  override def beforeEach() = {
    val uuid = java.util.UUID.randomUUID().toString
    testEntityName = testEntityPrefix + "-" + uuid
    testMetricName = testMetricPrefix + "-" + uuid
  }

  // Note: the following three endpoints probably should not exist. All socrata-http url patterns match urls with extra segments
  "Retrieve /metrics/*/range/*" should "show the same results as /metrics/*/range" in {
    persistManyMetrics(1 to 10)
    val rangeResponse = getJSONResponse(s"metrics/$testEntityName/range?start=$testStart&end=$testEnd")
    val rangeWithExtraResponse = getJSONResponse(s"metrics/$testEntityName/range/whatever?start=$testStart&end=$testEnd")
    rangeWithExtraResponse.bodyString shouldBeJSON rangeResponse.bodyString
  }
  "Retrieve /metrics/*/series/*" should "show the same results as /metrics/*/series" in {
    persistManyMetrics(1 to 10)
    val seriesResponse = getJSONResponse(s"metrics/$testEntityName/series?period=MONTHLY&start=$testStart&end=$testEnd")
    val seriesWithExtraResponse = getJSONResponse(s"metrics/$testEntityName/series/whatever?period=MONTHLY&start=$testStart&end=$testEnd")
    seriesWithExtraResponse.bodyString shouldBeJSON seriesResponse.bodyString
  }
  "Retrieve /metrics/*/whatever (not /range or /series)" should "show the same results as /metrics/*" in {
    persistSingleMetric()
    val metricResponse = getJSONResponse(s"/metrics/$testEntityName?period=YEARLY&date=$testStart")
    val metricWithExtraResponse = getJSONResponse(s"/metrics/$testEntityName/whatever?period=YEARLY&date=$testStart")
    metricWithExtraResponse.bodyString shouldBeJSON metricResponse.bodyString
  }

  "Retrieve /metrics range with no range" should "be fail" in {
    val response = getJSONProtoResponse(s"/metrics/$testEntityName/range")
    response shouldHaveCode BadRequest
  }

  "Retrieve /metrics without specifying" should "be not found" in {
    val response = getJSONProtoResponse("/metrics")
    response shouldHaveCode NotFound
  }

  "Retrieve /metrics range with a range" should "succeed" in {
    val response = getJSONProtoResponse(s"/metrics/$testEntityName/range?start=$testStart&end=$testEnd")
    response shouldHaveCode Ok
    response.json.bodyString shouldBeJSON """{}"""
  }

  "Retrieve /metrics/* with no period" should "fail with error msg" in {
    val response = getJSONProtoResponse(s"/metrics/$testEntityName")
    response shouldHaveCode BadRequest
    response.json.bodyString shouldBeJSON """ { "error": 400, "message": "Parameter period required." } """
  }

  "Retrieve /metrics/* with bad period" should "fail with error msg" in {
    val response = getJSONProtoResponse(s"/metrics/$testEntityName?period=crud")
    response shouldHaveCode BadRequest
    response.json.bodyString shouldBeJSON """ { "error": 400, "message": "Unable to parse period : No period named crud" } """
  }

  "Retrieve /metrics/* with no date" should "fail with error msg" in {
    val response = getJSONProtoResponse(s"/metrics/$testEntityName?period=YEARLY")
    response shouldHaveCode BadRequest
    response.json.bodyString shouldBeJSON """ { "error": 400, "message": "Parameter date required." } """
  }

  "Retrieve /metrics/* with bad date" should "fail with error msg" in {
    val response = getJSONProtoResponse(s"/metrics/$testEntityName?period=YEARLY&date=crud")
    response shouldHaveCode BadRequest
    response.json.bodyString shouldBeJSON """ { "error": 400, "message": "Unable to parse date crud" } """
  }

  "Retrieve /metrics/* with valid period and date" should "succeed" in {
    val response = getJSONProtoResponse(s"/metrics/$testEntityName?period=YEARLY&date=$testStart")
    response shouldHaveCode Ok
    response.json.bodyString shouldBeJSON """{}"""
  }

  "Retrieve /metrics/*/series with no period" should "fail with error msg" in {
    val response = getJSONProtoResponse(s"/metrics/$testEntityName/range")
    response shouldHaveCode BadRequest
  }

  "Retrieve /metrics/*/series with no start" should "fail with error msg" in {
    val response = getJSONResponse(s"/metrics/$testEntityName/series?period=YEARLY")
    response.code.code should be (BadRequest.code)
    response.bodyString shouldBeJSON """ { "error": 400, "message": "Parameter start required." } """
  }

  "Retrieve /metrics/*/series with no end" should "fail with error msg" in {
    val response = getJSONResponse(s"/metrics/$testEntityName/series?period=YEARLY&start=$testStart")
    response.code.code should be (BadRequest.code)
    response.bodyString shouldBeJSON """ { "error": 400, "message": "Parameter end required." } """
  }

  "Retrieve /metrics/*/series with period, start, and end" should "succeed" in {
    val response = getJSONResponse(s"/metrics/$testEntityName/series?period=YEARLY&start=$testStart&end=$testEnd")
    response.code.code should be (Ok.code)
  }

  "Retrieve /metrics/*/range with no start" should "fail with error msg" in {
    val response = getJSONProtoResponse(s"/metrics/$testEntityName/range")
    response shouldHaveCode BadRequest
    response.json.bodyString shouldBeJSON """ { "error": 400, "message": "Parameter start required." } """
  }

  "Retrieve /metrics/*/range with no end" should "fail with error msg" in {
    val response = getJSONProtoResponse(s"/metrics/$testEntityName/range?start=$testStart")
    response shouldHaveCode BadRequest
    response.json.bodyString shouldBeJSON """ { "error": 400, "message": "Parameter end required." } """
  }

  "Retrieve /metrics/*/range with start and end" should "succeed" in {
    val response = getJSONProtoResponse(s"/metrics/$testEntityName/range?start=$testStart&end=$testEnd")
    response shouldHaveCode Ok
  }

  // Returns the JSON string representation of the metric added
  def persistSingleMetric(): String = {
    val metrics = new Metrics()
    metrics.put(testMetricName, new Metric(RecordType.ABSOLUTE, 1))
    dataStore.persist(testEntityName, testPersistedDateEpoch, metrics)

    """{ "%s": { "value": 1, "type": "absolute" } }""".format(testMetricName)
  }

  // Returns the JSON string representation of the metrics added
  def persistManyMetrics(metRange: Range): String = {
    val metrics = new Metrics()
    for (i <- metRange) {
      metrics.put(testMetricName + "-" + i, new Metric(RecordType.ABSOLUTE, 1))
    }
    dataStore.persist(testEntityName, testPersistedDateEpoch, metrics)

    "{" +
      metRange.map(i =>
        """ "%s-%d": { "value": 1, "type": "absolute" } """
          .format(testMetricName, i)
      ).mkString(", ") +
      "}"
  }

  "Retrieve /metrics/* after persisting" should "show persisted metric" in {
    val expected = persistSingleMetric()
    val response = getJSONProtoResponse(s"/metrics/$testEntityName?period=YEARLY&date=$testPersistedDate")
    response shouldHaveCode Ok
    response.json.bodyString shouldBeJSON expected
  }

  "Retrieve /metrics range after persisting" should "show persisted metrics" in {
    val expected = persistSingleMetric()
    val response = getJSONProtoResponse(s"metrics/$testEntityName/range?start=$testStart&end=$testEnd")
    response shouldHaveCode Ok
    response.json.bodyString shouldBeJSON expected
  }

  "Retrieve /metrics range after persisting multiple metrics" should "show multiple persisted metrics" in {
    val metRange = 1 to 10
    val expected = persistManyMetrics(metRange)
    val response = getJSONProtoResponse(s"metrics/$testEntityName/range?start=$testStart&end=$testEnd")
    response shouldHaveCode Ok
    response.json.bodyString shouldBeJSON expected
  }

  "Retrieve /metrics series after persisting" should "show persisted metrics" in {
    val expectedMetric = persistSingleMetric()

    val response = getJSONResponse(s"metrics/$testEntityName/series?period=MONTHLY&start=$testStart&end=$testEnd")
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

    val response = getJSONResponse(s"metrics/$testEntityName/series?period=MONTHLY&start=$testStart&end=$testEnd")
    response.code.code should be (Ok.code)

    val expectSeries1 = """ { "start" : -2678400000, "end" : -1, "metrics" : { } } """
    val expectSeries2 = """ { "start" : 0, "end" : 2678399999, "metrics" : %s } """.format(expectedMetrics)
    val expectSeries3 = """ { "start" : 2678400000, "end": 5097599999, "metrics" : { } } """
    val expected = s"[ $expectSeries1, $expectSeries2, $expectSeries3 ]"
    response.bodyString shouldBeJSON expected
  }
}
