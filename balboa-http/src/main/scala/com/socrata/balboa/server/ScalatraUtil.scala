package com.socrata.balboa.server

import javax.servlet.http.HttpServletRequest
import scala.collection.JavaConverters._

object ScalatraUtil {
  def getAccepts(req: HttpServletRequest): Seq[String] = {
    req.getHeaders("accept").asScala.toSeq
  }
}
