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
import com.socrata.balboa.server.{MetricsService, Constants, ServiceUtils}
import com.socrata.http.server.HttpResponse
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import org.codehaus.jackson.map.annotate.JsonSerialize
import org.codehaus.jackson.map.{ObjectMapper, SerializationConfig}

import scala.collection.JavaConverters._

class MetricsRest

object MetricsRest {

  /*
  - TODO : Migrate business logic to MetricsService
  - TODO : Move String constants to Constant.
   */

  val seriesMeter = com.yammer.metrics.Metrics.newTimer(classOf[MetricsRest], "series queries", TimeUnit.MILLISECONDS, TimeUnit.SECONDS)
  val rangeMeter = com.yammer.metrics.Metrics.newTimer(classOf[MetricsRest], "range queries", TimeUnit.MILLISECONDS, TimeUnit.SECONDS)
  val periodMeter = com.yammer.metrics.Metrics.newTimer(classOf[MetricsRest], "period queries", TimeUnit.MILLISECONDS, TimeUnit.SECONDS)

  val JSON = "application/json; charset=utf-8"
  val PROTOBUF = "application/x-protobuf"
  val ds = DataStoreFactory.get()

  def extractEntityId(req: HttpServletRequest): String = {
    req.getPathInfo.split('/').filterNot(_.isEmpty)(1)
  }

  def unacceptable =
    NotAcceptable ~> ContentType("application/json; charset=utf-8") ~> Content("""{"error": 406, "message": "Not acceptable."}""")

  def required(parameter: String) =
    BadRequest ~> ContentType("application/json; charset=utf-8") ~> Content("""{"error": 400, "message": "Parameter """ + parameter + """ required."}""")

  def malformedDate(parameter: String) =
    BadRequest ~> ContentType("application/json; charset=utf-8") ~> Content("""{"error": 400, "message": "Unable to parse date """ + JString(parameter).toString.drop(1).dropRight(1) + """"}""")

  def br(parameter: String, msg: String) =
    BadRequest ~> ContentType("application/json; charset=utf-8") ~> Content("""{"error": 400, "message": "Unable to parse """ + parameter + """ : """ + JString(msg).toString.drop(1).dropRight(1) + """"}""")

  /**
   * @param samplePeriod The sample period of the existing absolute metrics
   * @param targetPeriod The target period we want for the metrics sample
   * @return Return a Bad Request response that details sample and target period mismatch.
   */
  def illegalPeriodMatch(samplePeriod: Period, targetPeriod: Period) = BadRequest~>
    ContentType("application/json; charset=utf-8") ~>
    Content("""{"error": 400, "message": "Sample period """ + samplePeriod + """ is of a lower granularity level then target period """ + targetPeriod + """"}""")

  def get(req: HttpServletRequest): HttpResponse = {
    val entityId = extractEntityId(req)
    val qs = new QueryExtractor(req)
    val period = qs[Period]("period") match {
      case Some(Right(value)) => value
      case Some(Left(err)) => return br("period", err)
      case None => return required("period")
    }
    val date = qs[String]("date").getOrElse { return required("date") }.right.get
    val combine = qs[String]("combine").map(_.right.get)
    val field = qs[String]("field").map(_.right.get)

    val mediaType = bestMediaType(req, JSON, PROTOBUF).getOrElse { return unacceptable }

    val begin = System.currentTimeMillis()

    val range = DateRange.create(period, ServiceUtils.parseDate(date).getOrElse { return malformedDate(date) })

    try
    {
      val iter = ds.find(entityId, period, range.getStart, range.getEnd)
      var metrics = Metrics.summarize(iter)

      combine.foreach { c => metrics = metrics.combine(c) }
      field.foreach { f => metrics = metrics.filter(f) }

      render(mediaType, metrics)
    }
    finally
    {
      periodMeter.update(System.currentTimeMillis() - begin, TimeUnit.MILLISECONDS)
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
    val entityId = extractEntityId(req)
    val qs = new QueryExtractor(req)
    val start = qs[String]("start").getOrElse { return required("start") }.right.get
    val end = qs[String]("end").getOrElse { return required("end") }.right.get
    val combine = qs[String]("combine").map(_.right.get)
    val field = qs[String]("field").map(_.right.get)

    val startDate = ServiceUtils.parseDate(start).getOrElse { return malformedDate(start) }
    val endDate = ServiceUtils.parseDate(end).getOrElse { return malformedDate(end) }

    val mediaType = bestMediaType(req, JSON, PROTOBUF).getOrElse { return unacceptable }

    val begin = System.currentTimeMillis()

    try
    {

      val iter = ds.find(entityId, startDate, endDate)
      var metrics = Metrics.summarize(iter)

      combine.foreach { c => metrics = metrics.combine(c) }
      field.foreach { f => metrics = metrics.filter(f) }

      render(mediaType, metrics)
    }
    finally
    {
      rangeMeter.update(System.currentTimeMillis() - begin, TimeUnit.MILLISECONDS)
    }
  }

  def response(typ: String, body: Array[Byte]) =
    ContentType(typ) ~> Header("Content-length", body.length.toString) ~> Stream(_.write(body))

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
    val entityId = extractEntityId(req)
    val qs = new QueryExtractor(req)
    val period = qs[Period]("period") match {
      case Some(Right(value)) => value
      case Some(Left(err)) => return br("period", err)
      case None => return required("period")
    }
    val start = qs[String]("start").getOrElse { return required("start") }.right.get
    val end = qs[String]("end").getOrElse { return required("end") }.right.get

    val startDate = ServiceUtils.parseDate(start).getOrElse { return malformedDate(start) }
    val endDate = ServiceUtils.parseDate(end).getOrElse { return malformedDate(end) }

    bestMediaType(req, JSON).getOrElse { return unacceptable }

    val begin = System.currentTimeMillis()

    try
    {
      OK ~> response(JSON, renderJson(ds.slices(entityId, period, startDate, endDate)).getBytes(UTF_8))
    }
    finally
    {
      seriesMeter.update(System.currentTimeMillis() - begin, TimeUnit.MILLISECONDS)
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
  def absoluteSeries(req: HttpServletRequest): HttpResponse = {
    extractSeriesParameters(req) match {
      case Right(resp) => resp
      case Left((entityId, targetPeriod, startDate, endDate)) =>
        val qs = new QueryExtractor(req)
        val samplePeriod = qs[Period](Constants.SAMPLE_PERIOD_PARAM).getOrElse { return required(Constants.SAMPLE_PERIOD_PARAM) }.right.get
        val metricName = qs[String](Constants.METRIC_NAME_PARAM).getOrElse  { return required(Constants.METRIC_NAME_PARAM) }.right.get

        // Identify the whether the sample period is of higher granularity.
        new PeriodComparator().compare(samplePeriod, targetPeriod) match {
          case i: Int if i < 0 => // Error Sample period of lower granularity
            illegalPeriodMatch(samplePeriod, targetPeriod)
          case i: Int if i > 0 => // Sample period is of higher granularity.
            // Find all the absolute values for the sample period
            val sampleTimeSlices = MetricsService.series(entityId, samplePeriod, startDate, endDate)

            // Map the target period to a list of date ranges for the target period.
            val dates = new DateRange(startDate, endDate).toDates(targetPeriod).asScala.toList
            val dateRanges = dates.zip(dates.tail).map(tuple => new DateRange(tuple._1, tuple._2))

            val

            OK
          case i: Int if i == 0 =>
            // If the sample period same as target period
            // Fall back to the default behaviour.
            OK ~> renderJsonResponse(MetricsService.series(entityId, targetPeriod, startDate, endDate))
        }
    }
  }

  // ok, so this isn't actually correct.  Screw correct, I just want this to work.
  // TODO: Something that was never fixed and is kind of unecessary should be forever deleted.
  // TODO: What would determine best media type?
  def bestMediaType(req: HttpServletRequest, types: String*): Option[String] = {
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

  private def renderJson(obj: Any) =
  {
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

  /**
   * Extracts the base series query parameters from HttpServletRequest.
   *
   * @param req HttpRequest that encaptures all base series query parameters.
   * @return Either (Entity ID, Target [[Period]], Start [[Date]], End [[Date]]) on success or Htt Error response on failure.
   */
  private def extractSeriesParameters(req: HttpServletRequest): Either[(String, Period, Date, Date), HttpResponse] = {
    val entityId = extractEntityId(req)
    val qs = new QueryExtractor(req)
    val period: Period = qs[Period]("period") match {
      case Some(Right(value)) => value
      case Some(Left(err)) => return Right(br("period", err))
      case None => return Right(required("period"))
    }
    val start = qs[String]("start").getOrElse { return Right(required("start")) }.right.get
    val end = qs[String]("end").getOrElse { return Right(required("end")) }.right.get

    val startDate = ServiceUtils.parseDate(start).getOrElse { return Right(malformedDate(start)) }
    val endDate = ServiceUtils.parseDate(end).getOrElse { return Right(malformedDate(end)) }
    Left((entityId, period, startDate, endDate))
  }
}
