package com.socrata.balboa.server.rest

import java.util.regex.Pattern

import com.socrata.balboa.metrics.data.DataStoreFactory
import com.socrata.balboa.server.{Error, ResponseWithType}
import com.socrata.balboa.server.ResponseWithType._
import org.codehaus.jackson.map.annotate.JsonSerialize
import org.codehaus.jackson.map.{ObjectMapper, SerializationConfig}
import org.eclipse.jetty.http.HttpStatus.BAD_REQUEST_400
import org.scalatra.{BadRequest, Ok, Params}

import scala.collection.JavaConverters._
import scala.util.Try

// scalastyle:off return

object EntitiesRest {
  val dataStore = DataStoreFactory.get

  def apply(params: Params): ResponseWithType = {
    val predicate: (String => Boolean) =
      params.get("filter").map(Pattern.compile)
        .map { pattern => (str: String) => pattern.matcher(str).matches }.getOrElse(_ => true)

    val it = dataStore.entities().asScala.filter(predicate)
    // backwards-compatibility: -1 == no limit
    val limitString = params.getOrElse("limit", "-1")
    val limit = Try(limitString.toInt).recover({
      case err: NumberFormatException =>
        return ResponseWithType(json,
          BadRequest(Error(
            BAD_REQUEST_400,
            "Unable to parse limit as int")))
    }).get

    val limitedIt = if(limit != -1) it.take(limit) else it

    val mapper = new ObjectMapper
    mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true)
    mapper.getSerializationConfig.withSerializationInclusion(JsonSerialize.Inclusion.NON_NULL)

    ResponseWithType(json, Ok(mapper.writeValueAsString(limitedIt.asJava)))
  }
}
