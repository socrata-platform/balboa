package com.socrata.balboa.agent

import java.net.{URL, URLEncoder}
import java.util.Date

import com.socrata.balboa.metrics.{EntityJSON, Metric, MetricJSON}
import com.socrata.metrics.{IdParts, MetricQueue}
import com.stackmob.newman.ApacheHttpClient
import com.stackmob.newman.dsl.POST
import com.stackmob.newman.response.HttpResponseCode.Ok
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.json4s.jackson.JsonMethods.{pretty, render}
import org.json4s.{DefaultFormats, Extraction, Formats}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

case class HttpMetricQueue(balboaHttpURL: String, timeout: Duration, maxRetryWait: Duration)
  extends MetricQueue with StrictLogging {

  val StartingWaitDuration = 50.millis
  val Utf8 = "UTF_8"

  implicit val httpClient = new ApacheHttpClient
  implicit val jsonFormats: Formats = DefaultFormats
  implicit val executionContext = ExecutionContext.global

  logger info s"Initializing HttpMetricQueue targeting $balboaHttpURL"

  /**
   * Interface for receiving a Metric
   *
   * @param entity Entity which this Metric belongs to (ex: a domain).
   * @param name Name of the Metric to store.
   * @param value Numeric value of this metric.
   * @param timestamp Time when this metric was created.
   * @param recordType Type of metric to add, See [[Metric.RecordType]] for more information.
   */
  override def create(entity: IdParts,
             name: IdParts,
             value: Long,
             timestamp: Long = new Date().getTime,
             recordType: Metric.RecordType = Metric.RecordType.AGGREGATE): Unit = {

    val metricToWrite = EntityJSON(timestamp, Map(name.toString -> MetricJSON(value, recordType.toString)))
    val url = new URL(s"$balboaHttpURL/metrics/${URLEncoder.encode(entity.toString, Utf8)}")
    val request = POST(url).setBody(pretty(render(Extraction.decompose(metricToWrite))))

    var waitInLoop = StartingWaitDuration

    while (true) {
      val requestWithTimeout = Future { Await.result(request.apply, timeout) }

      requestWithTimeout.onComplete {
        case Success(response) =>
          val responseCode = response.code.code
          if (responseCode != Ok.code) {
            return
          }
          logger info s"HTTP POST to Balboa HTTP returned error code $responseCode: ${response.bodyString}"
        case Failure(failure) =>
          logger info s"HTTP POST to Balboa HTTP failed with ${failure.getMessage}"
      }

      Thread.sleep(waitInLoop.toMillis)
      waitInLoop = Math.min(maxRetryWait.toMillis, (waitInLoop * 2).toMillis).millis
    }
  }

  override def close(): Unit = {}
}
