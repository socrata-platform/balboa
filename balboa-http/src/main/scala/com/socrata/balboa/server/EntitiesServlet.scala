package com.socrata.balboa.server

import java.util.regex.Pattern
import javax.servlet.http.HttpServletRequest

import com.socrata.balboa.metrics.data.{DataStoreFactory, DefaultDataStoreFactory}
import com.socrata.balboa.server.ResponseWithType.json
import com.typesafe.scalalogging.StrictLogging
import org.codehaus.jackson.map.annotate.JsonSerialize
import org.codehaus.jackson.map.{ObjectMapper, SerializationConfig}
import org.eclipse.jetty.http.HttpStatus.BAD_REQUEST_400
import org.scalatra._

import scala.collection.JavaConverters._

class EntitiesServletWithDefaultDataStore extends EntitiesServlet(DefaultDataStoreFactory)

class EntitiesServlet(dataStoreFactory: DataStoreFactory) extends ScalatraServlet
    with SocrataMetricsSupport
    with ClientCounter
    with StrictLogging
    with RequestLogger
    with JacksonJsonServlet
    with NotFoundFilter
    with UnexpectedErrorFilter {

  val dataStore = dataStoreFactory.get

  def getAccepts(req: HttpServletRequest): Seq[String] = {
    req.getHeaders("accept").asScala.toSeq
  }

  // scalastyle:off return
  get("*")(getEntities)
  def getEntities: ActionResult = {
    contentType = json

    val predicate: (String => Boolean) =
      params.get("filter").map(Pattern.compile)
        .map { pattern => (str: String) => pattern.matcher(str).matches }.getOrElse(_ => true)

    timer("entities-get")({
      val it = dataStore.entities().filter(predicate)
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
