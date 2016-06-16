package com.socrata.balboa.server

import org.scalatra.{ActionResult, BadRequest, NotAcceptable}

case class ResponseWithType(contentType: String, result: ActionResult)
case class Error(error: Int, message: String)

object ResponseWithType {
  val json = "application/json; charset=utf-8"
  val protobuf = "application/x-protobuf"

  def unacceptable: ResponseWithType =
    ResponseWithType(json, NotAcceptable(Error(406, "Not acceptable.")))

  def required(parameter: String): ResponseWithType =
    ResponseWithType(json, BadRequest(Error(400, s"Parameter $parameter required.")))

  def malformedDate(parameter: String): ResponseWithType =
    ResponseWithType(json, BadRequest(Error(400, "Unable to parse date " + parameter)))

  def br(parameter: String, msg: String): ResponseWithType =
    ResponseWithType(json, BadRequest(Error(400, s"Unable to parse $parameter : " + msg)))
}
