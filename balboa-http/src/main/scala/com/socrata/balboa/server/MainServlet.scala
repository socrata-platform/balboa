package com.socrata.balboa.server

import java.util.regex.Pattern
import javax.servlet.http.HttpServletRequest

import com.socrata.balboa.BuildInfo
import com.socrata.balboa.metrics.data.{BalboaFastFailCheck, DataStoreFactory}
import com.socrata.balboa.server.ResponseWithType.json
import org.codehaus.jackson.map.annotate.JsonSerialize
import org.codehaus.jackson.map.{ObjectMapper, SerializationConfig}
import org.eclipse.jetty.http.HttpStatus.BAD_REQUEST_400
import com.typesafe.scalalogging.StrictLogging
import org.json4s.{DefaultFormats, Formats}
import org.scalatra._
import org.scalatra.json.JacksonJsonSupport
import org.scalatra.metrics.MetricsSupport

import scala.collection.JavaConverters._

class MainServlet extends ScalatraServlet
    with MetricsSupport
    with ClientCounter
    with StrictLogging
    with RequestLogger
    with JacksonJsonSupport
    with NotFoundFilter
    with UnexpectedErrorFilter {

  override protected implicit val jsonFormats: Formats = DefaultFormats

  val dataStore = DataStoreFactory.get
  val versionString = BuildInfo.toJson

  def getAccepts(req: HttpServletRequest): Seq[String] = {
    req.getHeaders("accept").asScala.toSeq
  }

  get("/version*") {
    contentType = json
    if (BalboaFastFailCheck.getInstance.isInFailureMode) {
      InternalServerError
    } else {
      Ok(versionString)
    }
  }

  // scalastyle:off return
  get("/entities*")(getEntities)
  def getEntities: ActionResult = {
    contentType = json

    val predicate: (String => Boolean) =
      params.get("filter").map(Pattern.compile)
        .map { pattern => (str: String) => pattern.matcher(str).matches }.getOrElse(_ => true)

    timer("entities-get")({
      val it = dataStore.entities().asScala.filter(predicate)
      // backwards-compatibility: -1 == no limit
      val limitString = params.getOrElse("limit", "-1")
      val limit = try {
        limitString.toInt
      } catch {
        case err: NumberFormatException =>
          return BadRequest(Error(BAD_REQUEST_400, "Unable to parse limit as int"))
      }

      val limitedIt = if (limit != -1) it.take(limit) else it

      val mapper = new ObjectMapper
      mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true)
      mapper.getSerializationConfig.withSerializationInclusion(JsonSerialize.Inclusion.NON_NULL)

      Ok(mapper.writeValueAsString(limitedIt.asJava))
    }).call()
  }
}
