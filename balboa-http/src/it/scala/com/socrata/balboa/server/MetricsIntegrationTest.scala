package com.socrata.balboa.server

import java.net.URL

import com.stackmob.newman.ApacheHttpClient
import com.stackmob.newman.dsl.GET
import com.stackmob.newman.response.HttpResponseCode.{BadRequest, NotFound, Ok}
import org.json4s.jackson.JsonMethods.parse
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent._
import scala.language.postfixOps

class MetricsIntegrationTest extends FlatSpec with Matchers {
  implicit val httpClient = new ApacheHttpClient

  "Retrieve /metrics range with no range" should "be fail" in {
    val url = new URL(Config.Server, "/metrics/fake.name/range")
    val response = Await.result(GET(url).apply, Config.RequestTimeout)
    response.code.code should be (BadRequest.code)
  }

  "Retrieve /metrics without specifying" should "be not found" in {
    val url = new URL(Config.Server, "/metrics")
    val response = Await.result(GET(url).apply, Config.RequestTimeout)
    response.code.code should be (NotFound.code)
  }

  "Retrieve /metrics range with a range" should "succeed" in {
    val url = new URL(Config.Server, "/metrics/fake.name/range?start=2010-01-01+00%3A00%3A00%3A000&end=2017-01-01+00%3A00%3A00%3A000")
    val response = Await.result(GET(url).apply, Config.RequestTimeout)
    response.code.code should be (Ok.code)
    parse(response.bodyString) should be (parse("""{}"""))
  }
}
