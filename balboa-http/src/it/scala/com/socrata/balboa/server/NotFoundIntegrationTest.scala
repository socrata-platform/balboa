package com.socrata.balboa.server

import java.net.URL

import com.stackmob.newman.ApacheHttpClient
import com.stackmob.newman.dsl.GET
import com.stackmob.newman.response.HttpResponseCode.NotFound
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent._
import scala.language.postfixOps

class NotFoundIntegrationTest extends FlatSpec with Matchers {
  implicit val httpClient = new ApacheHttpClient

  "Retrieve /" should "be not found" in {
    val url = new URL(Config.Server, "/")
    val response = Await.result(GET(url).apply, Config.RequestTimeout)
    response.code.code should be (NotFound.code)
  }

  "Retrieve /abc" should "be not found" in {
    val url = new URL(Config.Server, "/abc")
    val response = Await.result(GET(url).apply, Config.RequestTimeout)
    response.code.code should be (NotFound.code)
  }

  "Retrieve /abc/def" should "be not found" in {
    val url = new URL(Config.Server, "/abc/def")
    val response = Await.result(GET(url).apply, Config.RequestTimeout)
    response.code.code should be (NotFound.code)
  }
}
