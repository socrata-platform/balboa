package com.socrata.balboa.server.rest

import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.TimeUnit

import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.metrics.{Metric, Metrics}
import com.socrata.balboa.metrics.data.{DataStoreFactory, DateRange, Period}
import com.socrata.balboa.metrics.impl.ProtocolBuffersMetrics
import com.socrata.balboa.server.{EntityJSON, ResponseWithType, ServiceUtils}
import com.socrata.balboa.server.ResponseWithType._
import org.codehaus.jackson.map.annotate.JsonSerialize
import org.codehaus.jackson.map.{ObjectMapper, SerializationConfig}
import org.scalatra.{Ok, Params}

class MetricsRest

object MetricsRest {
  val seriesMeter = com.yammer.metrics.Metrics.newTimer(classOf[MetricsRest], "series queries", TimeUnit.MILLISECONDS, TimeUnit.SECONDS)
  val rangeMeter = com.yammer.metrics.Metrics.newTimer(classOf[MetricsRest], "range queries", TimeUnit.MILLISECONDS, TimeUnit.SECONDS)
  val periodMeter = com.yammer.metrics.Metrics.newTimer(classOf[MetricsRest], "period queries", TimeUnit.MILLISECONDS, TimeUnit.SECONDS)
  val dataStore = DataStoreFactory.get()

  def get(entityId: String, params: Params, accepts: Seq[String]): ResponseWithType = {
    val period = params.get("period").map(Extractable[Period].extract) match {
      case Some(Right(value)) => value
      case Some(Left(err)) => return br("period", err)
      case None => return required("period")
    }

    val date = params.getOrElse("date", { return required("date") })
    val combine = params.get("combine")
    val field = params.get("field")

    val mediaType = bestMediaType(accepts, json, protobuf).getOrElse { return unacceptable }

    val begin = System.currentTimeMillis()

    val range = DateRange.create(period, ServiceUtils.parseDate(date).getOrElse { return malformedDate(date) })

    try
    {
      val iter = dataStore.find(entityId, period, range.start, range.end)
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

  def post(entityId: String, entityOpt: Option[EntityJSON]): ResponseWithType = {
    val entity = entityOpt.getOrElse({ return required("entity") })
    val metrics = new Metrics()
    for ((name, metric) <- entity.metrics) {
      val recordType = metric.`type` match {
        case "ABSOLUTE" => RecordType.ABSOLUTE
        case "AGGREGATE" => RecordType.AGGREGATE
        case _ => return br("metric type", "must be ABSOLUTE or AGGREGATE")
      }
      metrics.put(name, new Metric(recordType, metric.value))
    }
    dataStore.persist(entityId, entity.timestamp, metrics)
    ResponseWithType(json, Ok())
  }

  def range(entityId: String, params: Params, accepts: Seq[String]): ResponseWithType = {
    val start = params.getOrElse(("start"), { return required("start") })
    val end = params.getOrElse(("end"), { return required("end") })
    val combine = params.get("combine")
    val field = params.get("field")

    val startDate = ServiceUtils.parseDate(start).getOrElse { return malformedDate(start) }
    val endDate = ServiceUtils.parseDate(end).getOrElse { return malformedDate(end) }

    val mediaType = bestMediaType(accepts, json, protobuf).getOrElse { return unacceptable }

    val begin = System.currentTimeMillis()

    try
    {
      val iter = dataStore.find(entityId, startDate, endDate)
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

  def series(entityId: String, params: Params, accepts: Seq[String]): ResponseWithType = {
    val period = params.get("period").map(Extractable[Period].extract) match {
      case Some(Right(value)) => value
      case Some(Left(err)) => return br("period", err)
      case None => return required("period")
    }
    val start = params.getOrElse("start", { return required("start") })
    val end = params.getOrElse("end", { return required("end") })

    val startDate = ServiceUtils.parseDate(start).getOrElse { return malformedDate(start) }
    val endDate = ServiceUtils.parseDate(end).getOrElse { return malformedDate(end) }

    bestMediaType(accepts, json).getOrElse { return unacceptable }

    val begin = System.currentTimeMillis()

    try
    {
      val body = renderJson(dataStore.slices(entityId, period, startDate, endDate)).getBytes(UTF_8)
      ResponseWithType(json, Ok(body))
    }
    finally
    {
      seriesMeter.update(System.currentTimeMillis() - begin, TimeUnit.MILLISECONDS)
    }
  }

  // ok, so this isn't actually correct.  Screw correct, I just want this to work.
  def bestMediaType(accepts: Seq[String], types: String*): Option[String] = {
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

  private def render(format: String, metrics: Metrics): ResponseWithType = {
    val bytes = if(format == protobuf) {
      val mapper = new ProtocolBuffersMetrics
      mapper.merge(metrics)
      mapper.serialize
    } else {
      renderJson(metrics).getBytes(UTF_8)
    }
    ResponseWithType(format, Ok(bytes))
  }

  private def renderJson(obj: Any) =
  {
    val mapper = new ObjectMapper()
    mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true)
    mapper.getSerializationConfig.withSerializationInclusion(JsonSerialize.Inclusion.NON_NULL)
    mapper.configure(SerializationConfig.Feature.WRITE_ENUMS_USING_TO_STRING, true)

    mapper.writeValueAsString(obj)
  }
}
