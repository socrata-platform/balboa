package com.socrata.metrics

/**
 * A way of statically typing some of the entity/metric name keys
 */
sealed class IdParts(val _parts:Seq[MetricIdPart] = Seq()) {
  override def toString():String = _parts.mkString("")
  def getParts = _parts
  def replacePart(in:MetricIdPart, out:MetricIdPart) = {
    MetricIdParts( getParts.map { p:MetricIdPart => if (p == in) out else p }:_* )  // magic vargs
  }
  def replaceFirstUnresolved(part:MetricIdPart) = {
    MetricIdParts( getParts.map { p:MetricIdPart => if (p.isUnresolved()) part else p }:_* )
  }
  def hasPart(part:MetricIdPart) = {
    getParts != null && getParts.exists {
      e:MetricIdPart => e == part
    }
  }
  def isUnresolved() = {
    getParts != null && getParts.exists {
      e:MetricIdPart => e.toString().startsWith("%") && e.toString().endsWith("%")
    }
  }
}
sealed class MetricIdPart(val part:String) extends IdParts() {
  override def toString():String = if (part == null) "%unknown%" else part
  override def getParts = Seq(this)
}
case class MetricIdParts(p:MetricIdPart *) extends IdParts(p)
case class ViewUid(viewUid:String) extends MetricIdPart(viewUid)
case class UserUid(viewUid:String) extends MetricIdPart(viewUid)
case class DomainId(domainId:Int) extends MetricIdPart(String.valueOf(domainId))
case class ReferrerUri(referrer:String)
  extends MetricIdPart(if (referrer.length > ReferrerUri.MAX_URL_SIZE) referrer.substring(0, ReferrerUri.MAX_URL_SIZE)
                       else referrer)
case class QueryString(query:String) extends MetricIdPart(query)
case class AppToken(token:String) extends MetricIdPart(token)
case class Ip(ip:String) extends MetricIdPart(ip)
case class Host(host:String) extends MetricIdPart(host)
case class Path(path:String) extends MetricIdPart(path)
case class Fluff(fluff:String) extends MetricIdPart(fluff)

object ReferrerUri {
  final val MAX_URL_SIZE: Int = 500
}

