package com.socrata.balboa.agent

import java.net.URL

import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.metrics.{DomainId, MetricIdParts, UserUid, ViewUid}
import com.stackmob.newman.{ApacheHttpClient, Headers, RawBody}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.mockito.ArgumentCaptor
import org.scalatest.{BeforeAndAfterEach, ShouldMatchers, WordSpec}
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.mock.MockitoSugar

import com.socrata.balboa.util.TestUtil._

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Unit Tests for [[MetricConsumer]]
  */
class HttpMetricQueueSpec extends WordSpec
  with BeforeAndAfterEach
  with ShouldMatchers
  with MockitoSugar
  with StrictLogging {

  implicit val execContext = scala.concurrent.ExecutionContext.global

  val TestUrl = "http://www.google.com"
  val Timeout = 20.millis
  val MaxRetryWait = 500.millis

  val TestEntity = MetricIdParts(DomainId(42), UserUid("EntityUser"), ViewUid("EntityView"))
  val TestName = MetricIdParts(DomainId(55), UserUid("NameUser"), ViewUid("NameView"))
  val TestVal = 67
  val TestTime = 82
  val TestType = RecordType.ABSOLUTE

  var mockHttpClient = mock[ApacheHttpClient]
  var httpMetricQueue = new HttpMetricQueue(TestUrl, Timeout, MaxRetryWait, mockHttpClient)
  override def beforeEach(): Unit = {
    mockHttpClient = mock[ApacheHttpClient]
    httpMetricQueue = new HttpMetricQueue(TestUrl, Timeout, MaxRetryWait, mockHttpClient)
  }

  "An HttpMetricQueue" when {
    "a metric is created" should {
      "make an http request with the proper arguments" in {
        val createFut = Future { httpMetricQueue.create(TestEntity, TestName, TestVal, TestTime, TestType) }

        val url = ArgumentCaptor.forClass(classOf[URL])
        val body = ArgumentCaptor.forClass(classOf[RawBody])

        verify(mockHttpClient, timeout(5000)).post(url.capture(), notNull(classOf[Headers]), body.capture())

        url.getValue should be (new URL(s"$TestUrl/metrics/$TestEntity"))

        val bodyString = new String(body.getValue.map(_.toChar))
        bodyString shouldBeJSON s""" { "timestamp": $TestTime, "metrics": { "$TestName": { "value": $TestVal, "type": "$TestType" } } } """
      }
    }
  }
}
