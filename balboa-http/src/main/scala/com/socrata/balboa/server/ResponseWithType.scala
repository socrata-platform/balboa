package com.socrata.balboa.server

import org.eclipse.jetty.http.HttpStatus._
import org.scalatra.{ActionResult, BadRequest, NotAcceptable}

case class ResponseWithType(contentType: String, result: ActionResult)
case class Error(error: Int, message: String)

object ResponseWithType {
  val json = "application/json; charset=utf-8"
  val protobuf = "application/x-protobuf"

  def unacceptable: ResponseWithType =
    ResponseWithType(json, NotAcceptable(Error(NOT_ACCEPTABLE_406, "Not acceptable.")))

  def required(parameter: String): ResponseWithType =
    ResponseWithType(json, BadRequest(Error(BAD_REQUEST_400, s"Parameter $parameter required.")))

  def malformedDate(parameter: String): ResponseWithType =
    ResponseWithType(json, BadRequest(Error(BAD_REQUEST_400, "Unable to parse date " + parameter)))

  def badRequest(parameter: String, msg: String): ResponseWithType =
    ResponseWithType(json, BadRequest(Error(BAD_REQUEST_400, s"Unable to parse $parameter : " + msg)))
}
