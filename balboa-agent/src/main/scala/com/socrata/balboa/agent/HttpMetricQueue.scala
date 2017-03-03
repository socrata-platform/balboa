package com.socrata.balboa.agent

import java.net.{URL, URLEncoder}
import java.util.Date

import com.socrata.balboa.metrics.{EntityJSON, Metric, MetricJSON}
import com.socrata.metrics.{IdParts, MetricQueue}
import com.stackmob.newman.dsl.POST
import com.stackmob.newman.response.HttpResponseCode.Ok
import com.stackmob.newman.{ApacheHttpClient, HttpClient}
import com.typesafe.scalalogging.StrictLogging
import org.json4s.jackson.JsonMethods.{pretty, render}
import org.json4s.{DefaultFormats, Extraction, Formats}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor}
import scala.util.{Failure, Success, Try}

/**
  * Utility for persisting metrics to balboa-http
  *
  * @param balboaHttpURL Base url of balboa-http
  * @param timeout Duration to wait before a request should be considered failed
  * @param maxRetryWait Maximum delay between retries
  * @param maxRetries Max number times to retry the HTTP request
  * @param httpClient The HttpClient to use for making requests to balboa-http
  */
case class HttpMetricQueue(balboaHttpURL: String,
                           timeout: Duration,
                           maxRetryWait: Duration,
                           maxRetries: Option[Int] = None,
                           implicit val httpClient: HttpClient = new ApacheHttpClient)
  extends MetricQueue with StrictLogging {

  val StartingWaitDuration: FiniteDuration = 50.millis
  val Utf8 = "UTF-8"

  implicit val jsonFormats: Formats = DefaultFormats
  implicit val executionContext: ExecutionContextExecutor = ExecutionContext.global

  logger.info(s"Initializing HttpMetricQueue targeting $balboaHttpURL")

  /**
   * Accepts a metric to transmit to the remote metric store.
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

    // Originally the transport mechanism for publishing metrics from
    // balboa-agent to the metric store was ActiveMQ. ActiveMQ has a built in
    // infinite retry for publishing to the queue. During the transition from
    // ActiveMQ to sending metrics via HTTP, a strategy of replicating
    // ActiveMQ's behavior of getting stuck here and infinitely retrying was
    // taken to make the code transition simpler.
    //
    // An improvement on this naive infinite retry policy could go one of two ways:
    // 1. Adapt `balboa-http` to accept all transmissions and persist the ones
    //    it can't parsed, retrying them later (when it's been upgraded for
    //    example). This would safer for keeping accurate metrics.
    // 2. Make `balboa-agent` respond differently to different HTTP error
    //    codes. Possibly, for example (but not exhaustively) 404, 500s are
    //    infinite retry, but 400s are written to a file to be retried in the
    //    future.
    var i = 0
    while (maxRetries.forall(i < _)) {
      val requestWithTimeout = Try(Await.result(request.apply, timeout))

      requestWithTimeout match {
        case Success(response) =>
          val responseCode = response.code.code
          if (responseCode == Ok.code) {
            return // scalastyle:ignore return
          }
          logger info s"HTTP POST to Balboa HTTP returned error code $responseCode: ${response.bodyString}"
        case Failure(failure) =>
          logger info s"HTTP POST to Balboa HTTP failed with ${failure.getMessage}"
      }

      Thread.sleep(waitInLoop.toMillis)
      waitInLoop = Math.min(maxRetryWait.toMillis, (waitInLoop * 2).toMillis).millis

      i += 1
    }
  }

  override def close(): Unit = {}
}
