package com.socrata.balboa.server

import java.net.URL

import com.stackmob.newman.ApacheHttpClient
import com.stackmob.newman.dsl.GET
import com.stackmob.newman.response.HttpResponseCode.Ok
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent._
import scala.language.postfixOps

class EntitiesIntegrationTest extends FlatSpec with Matchers {
  implicit val httpClient = new ApacheHttpClient

  "Retrieve /entities" should "be successful" in {
    val url = new URL(Config.Server, "/entities")
    val response = Await.result(GET(url).apply, Config.RequestTimeout)
    response.code.code should be (Ok.code)
  }

  "Retrieve /entities/abc" should "be found" in {
    val url = new URL(Config.Server, "/entities/abc")
    val response = Await.result(GET(url).apply, Config.RequestTimeout)
    response.code.code should be (Ok.code)
  }
}
