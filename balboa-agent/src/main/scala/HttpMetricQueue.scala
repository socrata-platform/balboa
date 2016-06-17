package com.socrata.balboa.agent

import java.net.URL
import java.util.Date

import com.socrata.balboa.metrics.{EntityJSON, Metric, MetricJSON}
import com.socrata.metrics.{IdParts, MetricQueue}
import com.stackmob.newman.ApacheHttpClient
import com.stackmob.newman.dsl.POST
import com.stackmob.newman.response.HttpResponseCode.Ok
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.json4s.{DefaultFormats, Extraction, Formats}
import org.json4s.jackson.JsonMethods.{pretty, render}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

case class HttpMetricQueue(balboaHttpURL: String, timeout: Duration) extends MetricQueue with StrictLogging {

  implicit val httpClient = new ApacheHttpClient
  implicit val jsonFormats: Formats = DefaultFormats
  implicit val executionContext = ExecutionContext.global

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

    // TODO: lazy load metrics for the same entity and send them over in batches
    val metricToWrite = EntityJSON(timestamp, Map(name.toString -> MetricJSON(value, recordType.toString)))

    val url = new URL(s"$balboaHttpURL/metrics/$entity")

    val request = POST(url).setBody(pretty(render(Extraction.decompose(metricToWrite)))).apply

    val requestWithTimeout = Future { Await.result(request, timeout) }

    requestWithTimeout.onComplete {
      case Success(response) =>
        val responseCode = response.code.code
        if (responseCode != Ok.code) {
          logger info s"HTTP POST to Balboa HTTP returned error code $responseCode: " + response.bodyString
        }
      case Failure(failure) =>
        logger info s"HTTP POST to Balboa HTTP failed with " + failure.getMessage
    }
  }

  override def close(): Unit = {}
}
