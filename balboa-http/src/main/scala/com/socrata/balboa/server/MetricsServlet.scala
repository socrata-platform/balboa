package com.socrata.balboa.server

import java.nio.charset.StandardCharsets.UTF_8

import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.metrics.data.{DataStoreFactory, DateRange, Period}
import com.socrata.balboa.metrics.impl.ProtocolBuffersMetrics
import com.socrata.balboa.metrics.{Metric, Metrics}
import com.socrata.balboa.server.ResponseWithType._
import com.socrata.balboa.server.ScalatraUtil.getAccepts
import com.socrata.balboa.server.rest.Extractable
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.codehaus.jackson.map.annotate.JsonSerialize
import org.codehaus.jackson.map.{ObjectMapper, SerializationConfig}
import org.scalatra.metrics.MetricsSupport
import org.scalatra.{ActionResult, NoContent, Ok}

class MetricsServlet extends JacksonJsonServlet
    with MetricsSupport
    with ClientCounter
    with StrictLogging
    with NotFoundFilter
    with UnexpectedErrorFilter {

  val dataStore = DataStoreFactory.get()

  val MaxLogLength = 1000000

  val StartKey = "start"
  val EndKey = "end"
  val PeriodKey = "period"
  val DateKey = "date"
  val CombineKey = "combine"
  val FieldKey = "field"

  // Match paths like /metrics/:entityId and /metrics/:entityId/whatever
  get("""^\/([^\/]+).*""".r)(getMetrics)
  def getMetrics: ActionResult = {
    val entityId = params("captures")

    val period = params.get(PeriodKey).map(Extractable[Period].extract) match {
      case Some(Right(value)) => value
      case Some(Left(err)) =>
        contentType = json
        return badRequest(PeriodKey, err).result
      case None =>
        contentType = json
        return required(PeriodKey).result
    }

    val date = params.getOrElse(DateKey, {
      contentType = json
      return required(DateKey).result
    })

    val combine = params.get(CombineKey)
    val field = params.get(FieldKey)

    val mediaType = bestMediaType(getAccepts(request), json, protobuf).getOrElse {
      contentType = json
      return unacceptable.result
    }

    val range = DateRange.create(period, ServiceUtils.parseDate(date).getOrElse {
      contentType = json
      return malformedDate(date).result
    })

    timer("period queries")({
      val iter = dataStore.find(entityId, period, range.start, range.end)
      var metrics = Metrics.summarize(iter)

      combine.foreach { c => metrics = metrics.combine(c) }
      field.foreach { f => metrics = metrics.filter(f) }

      val response = render(mediaType, metrics)
      contentType = response.contentType
      response.result
    }).call()
  }

  get("/:entityId/range*")(getRange)
  def getRange: ActionResult = {
    val entityId = params("entityId")

    val start = params.getOrElse(StartKey, {
      contentType = json
      return required(StartKey).result
    })
    val end = params.getOrElse(EndKey, {
      contentType = json
      return required(EndKey).result
    })
    val combine = params.get(CombineKey)
    val field = params.get(FieldKey)

    val startDate = ServiceUtils.parseDate(start).getOrElse({
      contentType = json
      return malformedDate(start).result
    })
    val endDate = ServiceUtils.parseDate(end).getOrElse({
      contentType = json
      return malformedDate(end).result
    })

    val mediaType = bestMediaType(getAccepts(request), json, protobuf).getOrElse({
      contentType = json
      return unacceptable.result
    })

    timer("range queries")({
      val iter = dataStore.find(entityId, startDate, endDate)
      var metrics = Metrics.summarize(iter)

      combine.foreach { c => metrics = metrics.combine(c) }
      field.foreach { f => metrics = metrics.filter(f) }

      val result = render(mediaType, metrics)
      contentType = result.contentType
      result.result
    }).call()
  }

  get("/:entityId/series*")(getSeries)
  def getSeries: ActionResult = {
    val entityId = params("entityId")

    val period = params.get(PeriodKey).map(Extractable[Period].extract) match {
      case Some(Right(value)) => value
      case Some(Left(err)) =>
        contentType = json
        return badRequest(PeriodKey, err).result
      case None =>
        contentType = json
        return required(PeriodKey).result
    }
    val start = params.getOrElse(StartKey, {
      contentType = json
      return required(StartKey).result
    })
    val end = params.getOrElse(EndKey, {
      contentType = json
      return required(EndKey).result
    })

    val startDate = ServiceUtils.parseDate(start).getOrElse({
      contentType = json
      return malformedDate(start).result
    })
    val endDate = ServiceUtils.parseDate(end).getOrElse({
      contentType = json
      return malformedDate(end).result
    })

    bestMediaType(getAccepts(request), json).getOrElse({
      contentType = json
      return unacceptable.result
    })

    timer("series queries")({
      val body = renderJson(dataStore.slices(entityId, period, startDate, endDate)).getBytes(UTF_8)
      val resp = ResponseWithType(json, Ok(body))
      contentType = resp.contentType
      resp.result
    }).call()
  }

  post("/:entityId")(postMetrics())
  def postMetrics(): ActionResult = {
    // Note: The `contentType` is used to determine how the response is to be
    // interpreted. For some unclear reason, it must be set -before- calling
    // extractOpt for the body of the request in order for it to take effect on
    // the response.
    contentType = json
    val entityId = params("entityId")
    val entityOpt = parsedBody.extractOpt[EntityJSON]

    val entity = entityOpt.getOrElse({
      val bodyToPrint = if (request.body.length < MaxLogLength) {
        request.body
      } else {
        "<truncated - body too large to log>"
      }
      logger error s"Unable to parse metrics to save. Received '$bodyToPrint'"
      return badRequest("message body", "unable to parse as metrics entity").result
    })
    val metrics = new Metrics()
    for ((name, metric) <- entity.metrics) {
      val recordType = metric.`type` match {
        case "ABSOLUTE" => RecordType.ABSOLUTE
        case "AGGREGATE" => RecordType.AGGREGATE
        case _ => return badRequest("metric type", "must be ABSOLUTE or AGGREGATE").result
      }
      metrics.put(name, new Metric(recordType, metric.value))
    }

    timer("metric entity post")({
      dataStore.persist(entityId, entity.timestamp, metrics)
    }).call()

    NoContent()
  }


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
