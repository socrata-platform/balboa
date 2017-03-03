package com.socrata.balboa.server

import java.net.URL

import com.stackmob.newman.ApacheHttpClient
import com.stackmob.newman.dsl.GET
import com.stackmob.newman.response.HttpResponseCode.Ok
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent._

class VersionIntegrationTest extends FlatSpec with Matchers {
  implicit val httpClient = new ApacheHttpClient

  "Retrieve /version" should "be successful" in {
    val url = new URL(Config.Server, "/version")
    val response = Await.result(GET(url).apply, Config.RequestTimeout)
    response.code.code should be (Ok.code)
  }

  // Note: this endpoint probably should not exist. All socrata-http url
  // patterns match urls with extra segments. This is preserved for the
  // purposes of compatibility while transitioning to Scalatra. Once that
  // transition is complete, an analysis of logs should determine if this is in
  // use anywhere and it can be removed. Proper response for this endpoint
  // should be a 404.
  "Retrieve /version/abc" should "be found" in {
    val url = new URL(Config.Server, "/version/abc")
    val response = Await.result(GET(url).apply, Config.RequestTimeout)
    response.code.code should be (Ok.code)
  }
}
