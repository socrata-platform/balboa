package com.socrata.balboa.server

import java.net.URL

import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.metrics.{Metric, Metrics}
import com.socrata.balboa.metrics.data.DataStoreFactory
import com.stackmob.newman.ApacheHttpClient
import com.stackmob.newman.dsl.GET
import com.stackmob.newman.response.HttpResponse
import com.stackmob.newman.response.HttpResponseCode.{BadRequest, NotFound, Ok}
import org.json4s.jackson.JsonMethods.parse
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Await
import scala.language.postfixOps

class MetricsIntegrationTest extends FlatSpec with Matchers {
  implicit val httpClient = new ApacheHttpClient

  def awaitResponse(url: URL): HttpResponse = {
    Await.result(GET(url).apply, Config.RequestTimeout)
  }

  "Retrieve /metrics range with no range" should "be fail" in {
    val url = new URL(Config.Server, "/metrics/fake.name/range")
    val response = awaitResponse(url)
    response.code.code should be (BadRequest.code)
  }

  "Retrieve /metrics without specifying" should "be not found" in {
    val url = new URL(Config.Server, "/metrics")
    val response = awaitResponse(url)
    response.code.code should be (NotFound.code)
  }

  "Retrieve /metrics range with a range" should "succeed" in {
    val url = new URL(Config.Server, "/metrics/fake.name/range?start=2010-01-01+00%3A00%3A00%3A000&end=2017-01-01+00%3A00%3A00%3A000")
    val response = awaitResponse(url)
    response.code.code should be (Ok.code)
    parse(response.bodyString) should be (parse("""{}"""))
  }

  "Retrieve /metrics" should "show persisted metrics" in {
    val testMetName = "testMetricsPersisted"
    val testEntName = "testMetricsEntity"
    val ds = DataStoreFactory.get()
    val metrics = new Metrics()

    metrics.put(testMetName, new Metric(RecordType.ABSOLUTE, 1))
    ds.persist(testEntName, 0, metrics)

    val url = new URL(Config.Server, "metrics/" + testEntName + "/range?start=1969-01-01&end=1970-02-02")
    val response = awaitResponse(url)
    response.code.code should be (Ok.code)
    parse(response.bodyString) should be (parse("""{ "testMetricsPersisted": { "value": 1, "type": "absolute" } }"""))

    metrics.remove(testMetName)
    ds.persist(testEntName, 0, metrics)
  }
}
