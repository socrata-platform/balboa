package com.socrata.balboa.server.rest

import java.util.regex.Pattern
import javax.servlet.http.HttpServletRequest

import com.socrata.balboa.metrics.data.DataStoreFactory
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.{HttpResponse, Service}
import org.codehaus.jackson.map.annotate.JsonSerialize
import org.codehaus.jackson.map.{ObjectMapper, SerializationConfig}

import scala.collection.JavaConverters._

object EntitiesRest extends Service[HttpServletRequest, HttpResponse] {
  val ds = DataStoreFactory.get

  def apply(req: HttpServletRequest): HttpResponse = {
    val qs = new QueryExtractor(req)
    val predicate: (String => Boolean) =
      qs[String]("filter").map(Pattern.compile).map { pattern => (str: String) => pattern.matcher(str).matches }.getOrElse(_ => true)

    val it = ds.entities().asScala.filter(predicate)
    // backwards-compatibility: -1 == no limit
    val limit = qs[Int]("limit", () => Some(-1)) match {
      case Some(l) => l
      case None =>
        return BadRequest ~> ContentType("application/json") ~> Content("""{"error": 400, "message": "Unable to parse limit"}""")
    }
    val limitedIt = if(limit != -1) it.take(limit) else it

    val mapper = new ObjectMapper
    mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true)
    mapper.getSerializationConfig.setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL)

    OK ~> ContentType("application/json; charset=utf-8") ~> Content(mapper.writeValueAsString(limitedIt.asJava))
  }
}
