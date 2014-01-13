package com.socrata.balboa.server.rest

import scala.collection.JavaConverters._
import com.socrata.http.server.{Service, HttpResponse}
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.socrata.balboa.metrics.data.DataStoreFactory
import java.util.regex.Pattern
import org.codehaus.jackson.map.{SerializationConfig, ObjectMapper}
import org.codehaus.jackson.map.annotate.JsonSerialize
import javax.servlet.http.HttpServletRequest
import com.rojoma.json.ast.JString

object EntitiesRest extends Service[HttpServletRequest, HttpResponse] {
  val ds = DataStoreFactory.get

  def apply(req: HttpServletRequest): HttpResponse = {
    val qs = new QueryExtractor(req)
    val predicate: (String => Boolean) =
      qs[String]("filter").map(_.right.get).map(Pattern.compile).map { pattern => (str: String) => pattern.matcher(str).matches }.getOrElse(_ => true)

    val it = ds.entities().asScala.filter(predicate)
    // backwards-compatibility: -1 == no limit
    val limit = qs[Int]("limit", -1) match {
      case Right(l) => l
      case Left(err) =>
        return BadRequest ~> ContentType("application/json") ~> Content("""{"error": 400, "message": "Unable to parse limit : """ + JString(err).toString.drop(1).dropRight(1) + """"}""")
    }
    val limitedIt = if(limit != -1) it.take(limit) else it

    val mapper = new ObjectMapper
    mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true)
    mapper.getSerializationConfig.setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL)

    OK ~> ContentType("application/json; charset=utf-8") ~> Content(mapper.writeValueAsString(limitedIt.asJava))
  }
}
