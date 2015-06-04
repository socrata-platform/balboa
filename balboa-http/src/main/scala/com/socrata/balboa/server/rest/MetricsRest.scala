package com.socrata.balboa.server.rest

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Date
import java.util.concurrent.TimeUnit
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}

import com.rojoma.json.ast.JString
import com.socrata.balboa.metrics.Metrics
import com.socrata.balboa.metrics.data.impl.PeriodComparator
import com.socrata.balboa.metrics.data.{DataStoreFactory, DateRange, Period}
import com.socrata.balboa.metrics.impl.ProtocolBuffersMetrics
import com.socrata.balboa.server.MetricsService
import com.socrata.http.server.HttpResponse
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import org.apache.commons.logging.LogFactory
import org.codehaus.jackson.map.annotate.JsonSerialize
import org.codehaus.jackson.map.{ObjectMapper, SerializationConfig}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
 * REST URL Parameter keys.
 */
object ParamKeys {

  val DATE = "date"
  val PERIOD = "period"
  val SAMPLE_PERIOD = s"sample_$PERIOD"
  val TARGET_PERIOD = s"target_$PERIOD"
  val COMBINE = "combine"
  val FIELD = "field"
  val START = "start"
  val END = "end"

}

class MetricsRest

object MetricsRest {

  private val Log = LogFactory.getLog(this.getClass)

  /*
  - TODO : Migrate business logic to MetricsService
  - TODO : Move String constants to Constant.
  - TODO : Potentially Migrate to a different Scala Web Framework.
  - TODO : Move Yammer metrics into a Timer method so we dont have try catches sprinkled everywhere.
          We are potentially querying for a lot of data.  We need to a framework that is proved to be concurrent.
   */

  val seriesMeter = com.yammer.metrics.Metrics.newTimer(classOf[MetricsRest], "series queries", TimeUnit.MILLISECONDS, TimeUnit.SECONDS)
  val rangeMeter = com.yammer.metrics.Metrics.newTimer(classOf[MetricsRest], "range queries", TimeUnit.MILLISECONDS, TimeUnit.SECONDS)
  val periodMeter = com.yammer.metrics.Metrics.newTimer(classOf[MetricsRest], "period queries", TimeUnit.MILLISECONDS, TimeUnit.SECONDS)
  val forceAggregateSeriesMeter = com.yammer.metrics.Metrics.newTimer(classOf[MetricsRest], "force aggregated series queries", TimeUnit.MILLISECONDS, TimeUnit.SECONDS)

  val JSON = "application/json; charset=utf-8"
  val JSON_MIME = Content(JSON)
  val PROTOBUF = "application/x-protobuf"

  // The data store to use.
  implicit val ds = DataStoreFactory.get()

//  // TODO This error response is unacceptable
//  def unacceptable =
//    NotAcceptable ~> ContentType("application/json; charset=utf-8") ~> Content("""{"error": 406, "message": "Not acceptable."}""")
//
//

  /**
   * Gets a specific Period Query.
   *
   * @param req [[HttpServletRequest]] that contains the query parameters.
   * @return HttpResponse error or success.
   */
  def get(req: HttpServletRequest): HttpResponse = {
    extractPeriodParameters(req) match {
      case Success(p) =>
        val mediaType = bestMediaType(req, JSON, PROTOBUF).getOrElse { return unacceptable("Illegal MIME Type") }

        // Start the timed meter for this request
        val begin = System.currentTimeMillis()
        MetricsService.period(p.entityID, p.date, p.period, p.combineFilter, p.metricFilter) match {
          case Success(metrics) =>
            periodMeter.update(System.currentTimeMillis() - begin, TimeUnit.MILLISECONDS)
            render(mediaType, metrics)
          case Failure(t) =>
            internalServerError(t.getMessage)
        }
      case Failure(t) => badRequest(t.getMessage)
    }
  }

  /**
   * Metrics query that finds the a series of metrics rolled up byan interval defined by the "period" parameter.
   *
   * Example:
   *  curl - curl http://(balboa host):(balboa port)/metrics/52/series?period=MONTHLY&start=2009-01-01&end=2015-01-01
   *  httpie - http http://(balboa host):(balboa port)/metrics/52/series period==MONTHLY start==2009-01-01 end==2015-01-01
   *
   *  Result response body = Array of time windows
   *  [ { "start": 140...,
   *      "end" : 140... ,
   *      "metrics" : { "metric-name" : { "value" : 32738,
   *                                      "type" : "aggregate" },
   *                  ...
   *                  }
   *    }, ... ]
   *
   * @param req HTTPRequest request that contains desired period,
   * @return HTTPResponse where on OK (Code == 200), body is array of time windowed metrics request.
   */
  def series(req: HttpServletRequest): HttpResponse = {
    extractSeriesParameters(req) match {
      case Success(p) =>
        bestMediaType(req, JSON).getOrElse { return unacceptable("Illegal MIME Type") }
        val begin = System.currentTimeMillis()
        MetricsService.series(p.entityID, p.dateRange, p.period) match {
          case Success(timeSlices) =>
            seriesMeter.update(System.currentTimeMillis() - begin, TimeUnit.MILLISECONDS)
            OK ~> response(JSON, renderJson(timeSlices).getBytes(UTF_8))
          case Failure(t) =>
            internalServerError(t.getMessage)
        }
      case Failure(t) => badRequest(t.getMessage)
    }
  }

  /**
   * Given a HTTP Request that represents a query over a particular range calculate the total of all the metrics
   * for a particular entity id.  For all aggregate metrics all records of that metric within that range will be summed.
   * For absolute metrics, the latest metric within that window will be utilized.
   *
   * @param req HTTP Servlet Request that contains the parameter data.
   * @return ProtocolBufferMetrics or JSON mapping of Metric Name => {value, type}
   */
  def range(req: HttpServletRequest): HttpResponse = {
    extractRangeParameters(req) match {
      case Success(p) =>
        val mediaType = bestMediaType(req, JSON, PROTOBUF).getOrElse { return unacceptable("Illegal MIME Type") }
        val begin = System.currentTimeMillis()
        MetricsService.range(p.entityID, p.dateRange, p.combineFilter, p.metricFilter) match {
          case Success(metrics) =>
            rangeMeter.update(System.currentTimeMillis() - begin, TimeUnit.MILLISECONDS)
            render(mediaType, metrics)
          case Failure(t) =>
            internalServerError(t.getMessage)
        }
      case Failure(t) => badRequest(t.getMessage)
    }
  }

  /**
   * Given a set of absolute metrics at a specific level of granularity, this method aggregates this set of absolute metrics
   * at a target granularity.
   *
   * Required HttpRequest parameters:
   *    period - The target period we want the aggregation at
   *    sample_period - The granularity that exists for the absolute metric.
   *    metric_name - The Metric name of this
   *
   * @param req The HTTP Request that contains
   * @return The response that embodies
   */
  def forceAggregateSeries(req: HttpServletRequest): HttpResponse = {
    extractForceAggregateSeriesParameters(req) match {
      case Success(p) =>
        bestMediaType(req, JSON).getOrElse { return unacceptable("Illegal MIME Type") }
        val begin = System.currentTimeMillis()
        MetricsService.forceAggregateSeries(p.entityID, p.dateRange, p.period, p.metricFilter,p.samplePeriod) match {
          case Success(timeSlices) =>
            forceAggregateSeriesMeter.update(System.currentTimeMillis() - begin, TimeUnit.MILLISECONDS)
            OK ~> response(JSON, renderJson(timeSlices).getBytes(UTF_8))
          case Failure(t) =>
            internalServerError(t.getMessage)
        }
      case Failure(t) => badRequest(t.getMessage)
    }
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////
  ////  Private Helper Methods
  ////////////////////////////////////////////////////////////////////////////////////////////////////////

  // ok, so this isn't actually correct.  Screw correct, I just want this to work.
  // TODO: Something that was never fixed and is kind of unecessary should be forever deleted.
  // TODO: Why are we handling this and why aren't we using existing HTTP Server that does all this tedious stuff already.
  private def bestMediaType(req: HttpServletRequest, types: String*): Option[String] = {
    val accepts = req.getHeaders("accept").asScala.map(_.toString).toSeq
    if(accepts.isEmpty) return Some(types(0))
    for {
      accept <- accepts
      typ <- types
    } {
      if(accept.contains(typ.replaceAll(";.*", ""))) return Some(typ)
    }
    if(accepts.exists(_.contains("*/*"))) return Some(types(0))
    None
  }

  ////  HTTP Request generation
  ////////////////////////////////////////////////////////////////////////////////////////////////////////

  private def response(typ: String, body: Array[Byte]) =
    ContentType(typ) ~> Header("Content-length", body.length.toString) ~> Stream(_.write(body))

  private def errorJSONString(code: Int, message: String) = s"""{"error": $code, "message": "$message"}"""

  private def unacceptable(message: String): HttpResponse = {
    NotAcceptable ~> JSON_MIME ~> Content(errorJSONString(406, message))
  }

  /**
   * @param message The message in the body of this request.
   * @return The [[HttpServletResponse]] with a error code of 400 and the argument message.
   */
  private def badRequest(message: String): HttpResponse = {
    // TODO I feel like this should be built into Socrata-HTTP or replaced with a different HTTP Server library.
    BadRequest ~> JSON_MIME ~> Content(errorJSONString(400, message))
  }

  /**
   * @param message The internal server message.
   * @return The [[HttpResponse]] that represents a 500 Internal Server Error.
   */
  private def internalServerError(message: String): HttpResponse = {
    InternalServerError ~> JSON_MIME ~> Content(errorJSONString(500, message))
  }

  ////  Response Rendering
  ////////////////////////////////////////////////////////////////////////////////////////////////////////

  private def render(format: String, metrics: Metrics): HttpResponse = {
    val bytes = if(PROTOBUF.equals(format)) {
      val mapper = new ProtocolBuffersMetrics
      mapper.merge(metrics)
      mapper.serialize
    } else {
      renderJson(metrics).getBytes(UTF_8)
    }
    OK ~> response(format, bytes)
  }

  private def renderJson(obj: Any) = {
    val mapper = new ObjectMapper()
    mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true)
    mapper.getSerializationConfig.setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL)
    mapper.configure(SerializationConfig.Feature.WRITE_ENUMS_USING_TO_STRING, true)
    mapper.writeValueAsString(obj)
  }

  /**
   * Renders a JSON Http response with the parameterized object as its body.
   *
   * @param obj Object to serialize to JSON.
   * @return HTTP Response with JSON Body.
   */
  private def renderJsonResponse(obj: Any) = response(JSON, renderJson(obj).getBytes(UTF_8))

  ////  Parameter Field Extraction Private Helpers
  ////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Extracts the entity ID from the URL.
   *
   * @param req Request to be used to extract ID from.
   * @return The entity ID.
   */
  private def extractEntityId(req: HttpServletRequest): String = req.getPathInfo.split('/').filterNot(_.isEmpty)(1)

  /**
   * Extracts the [[PeriodParameters]] within a [[HttpServletRequest]].
   */
  private def extractPeriodParameters(req: HttpServletRequest): Try[PeriodParameters] = {
    val qs = new QueryExtractor(req)
    val combineFilter = qs[String](ParamKeys.COMBINE, () => None)
    val metricFilter = qs[String](ParamKeys.FIELD, () => None)
    for {
      period <- qs[Period](ParamKeys.PERIOD)
      date <- qs[Date](ParamKeys.DATE)
    } yield PeriodParameters(extractEntityId(req), period, date, combineFilter, metricFilter)
  }

  /**
   * Extracts the [[SeriesParameters]] within a [[HttpServletRequest]].
   */
  private def extractSeriesParameters(req: HttpServletRequest): Try[SeriesParameters] = {
    val qs = new QueryExtractor(req)
    for {
      start <- qs[Date](ParamKeys.START)
      end <- qs[Date](ParamKeys.END)
      period <- qs[Period](ParamKeys.PERIOD)
    } yield SeriesParameters(extractEntityId(req), new DateRange(start, end), period)
  }

  /**
   * Extracts the [[RangeParameters]] within a [[HttpServletRequest]].
   */
  private def extractRangeParameters(req: HttpServletRequest): Try[RangeParameters] = {
    val qs = new QueryExtractor(req)
    val combineFilter = qs[String](ParamKeys.COMBINE, () => None)
    val metricFilter = qs[String](ParamKeys.FIELD, () => None)
    for {
      start <- qs[Date](ParamKeys.START)
      end <- qs[Date](ParamKeys.END)
    } yield RangeParameters(extractEntityId(req), new DateRange(start, end), combineFilter, metricFilter)
  }

  /**
   * Extracts the [[ForceAggregateSeriesParameters]] within a [[HttpServletRequest]].
   */
  private def extractForceAggregateSeriesParameters(req: HttpServletRequest): Try[ForceAggregateSeriesParameters] = {
    val qs = new QueryExtractor(req)
    for {
      start <- qs[Date](ParamKeys.START)
      end <- qs[Date](ParamKeys.END)
      period <- qs[Period](ParamKeys.PERIOD)
      samplePeriod <- qs[Period](ParamKeys.SAMPLE_PERIOD)
      metricFilter <- qs[String](ParamKeys.FIELD)
    } yield ForceAggregateSeriesParameters(extractEntityId(req), new DateRange(start, end), period, samplePeriod, metricFilter)
  }

}
