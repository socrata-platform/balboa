package com.socrata.balboa.server.rest

import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.TimeUnit
import javax.servlet.http.HttpServletRequest

import com.rojoma.json.v3.ast.JString
import com.socrata.balboa.metrics.Metrics
import com.socrata.balboa.metrics.data.{DataStoreFactory, DateRange, Period}
import com.socrata.balboa.metrics.impl.ProtocolBuffersMetrics
import com.socrata.balboa.server.ServiceUtils
import com.socrata.http.server.HttpResponse
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import org.codehaus.jackson.map.annotate.JsonSerialize
import org.codehaus.jackson.map.{ObjectMapper, SerializationConfig}

import scala.collection.JavaConverters._

class MetricsRest

object MetricsRest {
  val seriesMeter = com.yammer.metrics.Metrics.newTimer(classOf[MetricsRest], "series queries", TimeUnit.MILLISECONDS, TimeUnit.SECONDS)
  val rangeMeter = com.yammer.metrics.Metrics.newTimer(classOf[MetricsRest], "range queries", TimeUnit.MILLISECONDS, TimeUnit.SECONDS)
  val periodMeter = com.yammer.metrics.Metrics.newTimer(classOf[MetricsRest], "period queries", TimeUnit.MILLISECONDS, TimeUnit.SECONDS)

  val json = "application/json; charset=utf-8"
  val protobuf = "application/x-protobuf"
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

    val mediaType = bestMediaType(req, json, protobuf).getOrElse { return unacceptable }

    val begin = System.currentTimeMillis()

    val range = DateRange.create(period, ServiceUtils.parseDate(date).getOrElse { return malformedDate(date) })

    try
    {
      val iter = ds.find(entityId, period, range.start, range.end)
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

  def range(req: HttpServletRequest): HttpResponse = {
    val entityId = extractEntityId(req)
    val qs = new QueryExtractor(req)
    val start = qs[String]("start").getOrElse { return required("start") }.right.get
    val end = qs[String]("end").getOrElse { return required("end") }.right.get
    val combine = qs[String]("combine").map(_.right.get)
    val field = qs[String]("field").map(_.right.get)

    val startDate = ServiceUtils.parseDate(start).getOrElse { return malformedDate(start) }
    val endDate = ServiceUtils.parseDate(end).getOrElse { return malformedDate(end) }

    val mediaType = bestMediaType(req, json, protobuf).getOrElse { return unacceptable }

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

    bestMediaType(req, json).getOrElse { return unacceptable }

    val begin = System.currentTimeMillis()

    try
    {
      val body = renderJson(ds.slices(entityId, period, startDate, endDate)).getBytes(UTF_8)
      OK ~> response(json, body)
    }
    finally
    {
      seriesMeter.update(System.currentTimeMillis() - begin, TimeUnit.MILLISECONDS)
    }
  }

  // ok, so this isn't actually correct.  Screw correct, I just want this to work.
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
    val bytes = if(format == protobuf) {
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
}
