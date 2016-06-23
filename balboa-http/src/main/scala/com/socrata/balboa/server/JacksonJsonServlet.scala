package com.socrata.balboa.server

import org.json4s.{DefaultFormats, Formats}
import org.scalatra.ScalatraServlet
import org.scalatra.json.JacksonJsonSupport

trait JacksonJsonServlet extends ScalatraServlet
  with JacksonJsonSupport {

  override protected implicit val jsonFormats: Formats = DefaultFormats
}
