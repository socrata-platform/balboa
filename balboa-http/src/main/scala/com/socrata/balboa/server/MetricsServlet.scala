package com.socrata.balboa.server

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Date

import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.metrics.data.{DataStoreFactory, DateRange, DefaultDataStoreFactory, Period}
import com.socrata.balboa.metrics.impl.ProtocolBuffersMetrics
import com.socrata.balboa.metrics.{Metric, Metrics, Timeslice}
import com.socrata.balboa.server.ResponseWithType._
import com.socrata.balboa.server.ScalatraUtil.getAccepts
import com.socrata.balboa.server.rest.Extractable
import com.typesafe.scalalogging.StrictLogging
import org.codehaus.jackson.map.annotate.JsonSerialize
import org.codehaus.jackson.map.{ObjectMapper, SerializationConfig}
import org.scalatra.{ActionResult, NoContent, Ok}

import scala.collection.JavaConverters._

// scalastyle:off return

class MetricsServletWithDefaultDatastore extends MetricsServlet(DefaultDataStoreFactory)

class MetricsServlet(dataStoreFactory: DataStoreFactory) extends JacksonJsonServlet
    with SocrataMetricsSupport
    with ClientCounter
    with RequestLogger
    with StrictLogging
    with NotFoundFilter
    with UnexpectedErrorFilter {

  val dataStore = dataStoreFactory.get

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

    timer("metrics-get")({
      var metrics = Metrics.summarize(dataStore.find(entityId, period, range.start, range.end))

      combine.foreach { c => metrics = metrics.combine(c) }
      field.foreach { f => metrics = metrics.filter(f) }

      val response = render(mediaType, metrics)
      contentType = response.contentType
      response.result
    }).call()
  }

  private def rangeMetrics(entityId: String,
                           startDate: Date,
                           endDate: Date,
                           combine: Option[String],
                           field: Option[String]): Metrics = {
    var metrics = Metrics.summarize(dataStore.find(entityId, startDate, endDate))
    combine.foreach { c => metrics = metrics.combine(c) }
    field.foreach { f => metrics = metrics.filter(f) }
    metrics
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

    timer("metrics-get-range")({
      val metrics = rangeMetrics(entityId, startDate, endDate, combine, field)
      val result = render(mediaType, metrics)
      contentType = result.contentType
      result.result
    }).call()
  }

  get("/range")(getRanges)
  def getRanges: ActionResult = {
    val entityIds = multiParams("entityId")

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

    timer("metrics-get-ranges")({
      val metrics = entityIds.par.map(entityId =>
        (entityId, rangeMetrics(entityId, startDate, endDate, combine, field))).toMap
      // asJava is necessary for json4s/Jackson to serialize parallel collections to JSON correctly
      val body = renderJson(metrics.seq.asJava).getBytes(UTF_8)
      contentType = json
      Ok(body)
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

    timer("metrics-get-series")({
      val series = dataStore.slices(entityId, period, startDate, endDate)
      // asJava is necessary for json4s/Jackson to serialize parallel collections to JSON correctly
      val body = renderJson(series.seq.asJava).getBytes(UTF_8)
      contentType = json
      Ok(body)
    }).call()
  }

  get("/series")(getSerieses)
  def getSerieses: ActionResult = {
    val entityIds = multiParams("entityId")

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

    timer("metrics-get-series")({
      val serieses = entityIds.par.map(entityId =>
        (entityId, dataStore.slices(entityId, period, startDate, endDate))).toMap
      // asJava is necessary for json4s/Jackson to serialize parallel collections to JSON correctly
      val body = renderJson(serieses.map({ case (k, v) => (k, v.asJava) }).seq.asJava).getBytes(UTF_8)
      contentType = json
      Ok(body)
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
    for {
      (name, metric) <- entity.metrics
    } yield {
      val recordType = metric.`type` match {
        case "ABSOLUTE" => RecordType.ABSOLUTE
        case "AGGREGATE" => RecordType.AGGREGATE
        case _ => return badRequest("metric type", "must be ABSOLUTE or AGGREGATE").result
      }
      metrics.put(name, new Metric(recordType, metric.value))
    }

    timer("metric-post")({
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
