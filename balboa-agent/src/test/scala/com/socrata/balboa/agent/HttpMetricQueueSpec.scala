package com.socrata.balboa.agent

import java.net.{URL, URLEncoder}

import com.fasterxml.jackson.core.JsonParseException
import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.metrics.{DomainId, MetricIdParts, UserUid, ViewUid}
import com.stackmob.newman.{ApacheHttpClient, Headers, RawBody}
import com.typesafe.scalalogging.StrictLogging
import org.json4s._
import org.json4s.jackson.JsonMethods.{parse, pretty, render}
import org.mockito.ArgumentCaptor
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, ShouldMatchers, WordSpec}

import scala.concurrent.duration._
import scala.util.Try

/**
  * Unit Tests for [[MetricConsumer]]
  */
class HttpMetricQueueSpec extends WordSpec
  with BeforeAndAfterEach
  with ShouldMatchers
  with MockitoSugar
  with StrictLogging {

  // To remove once PR #6 lands in socrata-test-common
  import scala.language.implicitConversions
  class AssertionJSON(actual: => String) {
      def shouldBeJSON(expected: String) = {
        val actualObj = Try(parse(actual)).recover({ case jpe: JsonParseException =>
                          fail(s"""Unable to parse actual value "$actual" as JSON""", jpe)}).get
        val expectObj = Try(parse(expected)).recover({ case jpe: JsonParseException =>
                          fail(s"""Unable to parse expected value "$expected" as JSON""", jpe)}).get

        withClue(
          "\nTextual actual:\n\n" + pretty(render(actualObj)) + "\n\n\n" +
          "Textual expected:\n\n" + pretty(render(expectObj)) + "\n\n")
          { actualObj should be (expectObj) }
      }
  }
  implicit def convertJSONAssertion(j: => String): AssertionJSON = new AssertionJSON(j)


  implicit val execContext = scala.concurrent.ExecutionContext.global

  val TestUrl = "http://www.google.com"
  val Timeout = 20.millis
  val MaxRetryWait = 500.millis

  val TestTimeout = 5.seconds.toMillis

  val TestEntity = MetricIdParts(DomainId(42), UserUid("EntityUser"), ViewUid("EntityView"))
  val TestName = MetricIdParts(DomainId(55), UserUid("NameUser"), ViewUid("NameView"))
  val TestVal = 67
  val TestTime = 82
  val TestType = RecordType.ABSOLUTE
  val TestRetryNum = 3

  var mockHttpClient = mock[ApacheHttpClient]
  var httpMetricQueue = new HttpMetricQueue(TestUrl, Timeout, MaxRetryWait, Some(TestRetryNum), mockHttpClient)
  override def beforeEach(): Unit = {
    mockHttpClient = mock[ApacheHttpClient]
    httpMetricQueue = new HttpMetricQueue(TestUrl, Timeout, MaxRetryWait, Some(TestRetryNum), mockHttpClient)
  }

  "An HttpMetricQueue" when {
    "a metric is created" should {
      "make an http request with the proper arguments" in {
        httpMetricQueue.create(TestEntity, TestName, TestVal, TestTime, TestType)

        val url: ArgumentCaptor[URL] = ArgumentCaptor.forClass(classOf[URL])
        val body: ArgumentCaptor[RawBody] = ArgumentCaptor.forClass(classOf[RawBody])

        verify(mockHttpClient, atLeastOnce)
          .post(url.capture(), any[Headers](), body.capture())

        url.getValue should be (new URL(s"$TestUrl/metrics/${URLEncoder.encode(TestEntity.toString, "UTF-8")}"))

        val bodyString = new String(body.getValue.map(_.toChar))
        bodyString shouldBeJSON s""" { "timestamp": $TestTime, "metrics": { "$TestName": { "value": $TestVal, "type": "$TestType" } } } """
      }

      "retry when it doesn't hear back" in {
        httpMetricQueue.create(TestEntity, TestName, TestVal, TestTime, TestType)

        verify(mockHttpClient, times(TestRetryNum))
          .post(any(), any(), any())
      }
    }
  }
}
