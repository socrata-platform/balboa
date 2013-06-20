package com.socrata.metrics

/**
 * A way of statically typing some of the entity/metric name keys
 */
sealed class IdParts(val parts:Seq[MetricIdPart] = null) {
  override def toString():String = parts.mkString("")
}
sealed class MetricIdPart(val part:String) extends IdParts {
  override def toString():String = part
}
case class MetricIdParts(p:MetricIdPart *) extends IdParts(p)
case class ViewUid(viewUid:String) extends MetricIdPart(viewUid);
case class UserUid(viewUid:String) extends MetricIdPart(viewUid);
case class DomainId(domainId:Int) extends MetricIdPart(String.valueOf(domainId))
case class ReferrerUri(referrer:String)
  extends MetricIdPart(if (referrer.length > ReferrerUri.MAX_URL_SIZE) referrer.substring(0, ReferrerUri.MAX_URL_SIZE)
                       else referrer)
case class QueryString(query:String) extends MetricIdPart(query)
case class AppToken(token:String) extends MetricIdPart(token)
case class Ip(ip:String) extends MetricIdPart(ip)
case class Host(host:String) extends MetricIdPart(host)
case class Path(path:String) extends MetricIdPart(path)
case class Fluff(p:String) extends MetricIdPart(p)

object ReferrerUri {
  final val MAX_URL_SIZE: Int = 500
}

