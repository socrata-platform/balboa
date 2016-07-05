package com.socrata.metrics

import java.net.URL
import java.util.UUID

/**
 * A way of statically typing some of the entity/metric name keys
 */
sealed class IdParts(val _parts:Seq[MetricIdPart] = Seq()) {
  val Percent = "%" // scalastyle:ignore
  protected def percentAround(s: String): String = Percent + s + Percent

  override def toString: String = _parts.mkString("")
  def getParts: Seq[MetricIdPart] = _parts
  def replacePart(in:MetricIdPart, out:MetricIdPart): MetricIdParts = {
    MetricIdParts( getParts.map { p:MetricIdPart => if (p == in) { out } else { p } }:_* )  // magic vargs
  }
  def replaceFirstUnresolved(part:MetricIdPart): MetricIdParts = {
    MetricIdParts( getParts.map { p:MetricIdPart => if (p.isUnresolved) { part } else { p } }:_* )
  }
  def hasPart(part:MetricIdPart): Boolean = {
    Option(getParts) match {
      case Some(someParts) => someParts contains part
      case _ => false
    }
  }
  def isUnresolved: Boolean = {
    Option(getParts) match {
      case Some(someParts) => getParts.exists({
        e:MetricIdPart =>  e.toString().startsWith(Percent) && e.toString().endsWith(Percent)
      })
      case None => false
    }
  }
}
sealed class MetricIdPart(val part: String) extends IdParts() {
  override def toString: String = if (Option(part).isDefined) { percentAround("unknown") } else { part }
  override def getParts: Seq[MetricIdPart] = Seq(this)
}
case class MetricIdParts(p:MetricIdPart *) extends IdParts(p)
case class ViewUid(viewUid:String) extends MetricIdPart(viewUid)
case class UserUid(viewUid:String) extends MetricIdPart(viewUid)
case class DomainId(domainId:Int) extends MetricIdPart(String.valueOf(domainId))

case class ReferrerUri(referrer: String)
  extends MetricIdPart(
    if (referrer.length > ReferrerUri.MAX_URL_SIZE) {
      referrer.substring(0, ReferrerUri.MAX_URL_SIZE)
    } else {
      referrer
    }) {

  def isBlank(query:String): Boolean = {
    Option(query).isEmpty || query.trim.isEmpty
  }

  def getPath: String = {
    if (referrer.startsWith(Percent)) {
      percentAround("rpath:" + UUID.randomUUID().toString)
    } else {
      val url = new URL(referrer)

      if (!isBlank(url.getQuery)) {
        url.getPath + "?" + url.getQuery
      } else {
        url.getPath
      }
    }
  }

  def getHost: String = {
    if (referrer.startsWith(Percent)) {
      percentAround("rhost:" + UUID.randomUUID().toString)
    } else {
      val url = new URL(referrer)
      url.getProtocol + "-" + url.getHost
    }
  }
}
case class QueryString(query:String) extends MetricIdPart(query)
case class AppToken(token:String) extends MetricIdPart(token)
case class Ip(ip:String) extends MetricIdPart(ip)
case class Host(host:String) extends MetricIdPart(host)
case class Path(path:String) extends MetricIdPart(path)

/**
  * Only generic Metric Id part.
  *
  * @param fluff literal String to be used for this Id.
  */
case class Fluff(fluff:String) extends MetricIdPart(fluff)
case class Unknown() extends MetricIdPart("unknown")

object ReferrerUri {
  final val MAX_URL_SIZE: Int = 500 // scalastyle:ignore
}

