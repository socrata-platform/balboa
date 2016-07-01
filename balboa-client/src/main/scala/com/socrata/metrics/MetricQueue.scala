package com.socrata.metrics

import java.util.Date

import com.socrata.balboa.metrics.Metric
import com.socrata.metrics.MetricQueue.{AccessChannel, Action}

object MetricQueue {

  // scalastyle: off field.name
  val AGGREGATE_GRANULARITY: Long = 120 * 1000

  object Action extends Enumeration {
    type Action = Value
    val SHARE = Value("share")
    val COMMENT = Value("comment")
    val RATING = Value("rating")
    val VIEW = Value("view")
    val FAVORITE = Value("favorite")
  }

  object Import extends Enumeration {
    type Import = Value
    val REPLACE = Value("replace")
    val IMPORT = Value("import")
    val APPEND = Value("append")
  }

  object AccessChannel extends Enumeration {
    type AccessChannel = Value
    val DOWNLOAD = Value("download")
    val WEBSITE = Value("website")
    val API = Value("api")
    val PRINT = Value("print")
    val WIDGET = Value("widget")
    val RSS = Value("rss")
    val EMAIL = Value("email")
    val UNKNOWN = Value("unknown")
  }

}

/**
 * Base interface that all metrics producers must implement.
 */
trait MetricQueue extends AutoCloseable {

  /**
   * Interface for receiving a Metric
   *
   * @param entity Entity which this Metric belongs to (ex: a domain).
   * @param name Name of the Metric to store.
   * @param value Numeric value of this metric.
   * @param timestamp Time when this metric was created.
   * @param recordType Type of metric to add, See [[Metric.RecordType]] for more information.
   */
  def create(entity: IdParts, name: IdParts, value: Long,
             timestamp: Long = new Date().getTime, recordType: Metric.RecordType = Metric.RecordType.AGGREGATE): Unit

}

/**
 * TODO: This Class should die.  Socrata specific class in Open Source Project needs to go Away.
 */
trait SocrataMetricQueue extends MetricQueue {

  // TODO: Convert display type to enum
  def logViewChildLoaded(parentViewUid:ViewUid, childViewUid:ViewUid, displaytype: String): Unit = {
    if (Option(displaytype).isEmpty || displaytype.isEmpty) {
      create(MetricIdParts(Fluff("children-loaded-"), parentViewUid), MetricIdParts(Fluff("filter-"), childViewUid), 1)
    }
    else if (displaytype == "map") {
      create(MetricIdParts(Fluff("children-loaded-"), parentViewUid), MetricIdParts(Fluff("map-" ), childViewUid), 1)
    }
    else if (displaytype == "chart") {
      create(MetricIdParts(Fluff("children-loaded-"), parentViewUid), MetricIdParts(Fluff("chart-"), childViewUid), 1)
    }
  }

  def logViewSearch(view:ViewUid, query:QueryString): Unit = {
    create(MetricIdParts(Fluff("searches-"), view), MetricIdParts(Fluff("search-"), query), 1)
  }

  def logUserSearch(domainId:DomainId, query:QueryString): Unit = {
    create(MetricIdParts(Fluff("searches-"), domainId), MetricIdParts(Fluff("users-search-"), query), 1)
  }

  def logDatasetSearch(domainId:DomainId, query:QueryString): Unit = {
    create(MetricIdParts(Fluff("searches-"), domainId), MetricIdParts(Fluff("datasets-search-"), query), 1)
  }

  def logUserCreated(domainId:DomainId): Unit = {
    create(domainId, Fluff("users-created"), 1)
  }

  def logAppTokenCreated(domainId:DomainId): Unit = {
    create(domainId, Fluff("app-token-created"), 1)
  }

  def logAppTokenRequest(tokenUid:AppToken, domainId:DomainId, ip:Ip): Unit = {
    create(Fluff("ip-applications"), MetricIdParts(Fluff("application-"), tokenUid, Fluff("-"),  ip), 1)
    create(MetricIdParts(domainId, Fluff("-applications")), MetricIdParts(Fluff("application-"),tokenUid), 1)
    create(MetricIdParts(Fluff("applications")), MetricIdParts(Fluff("application-"), tokenUid), 1)
    create(MetricIdParts(Fluff("application-"),tokenUid), MetricIdParts(Fluff("requests")), 1)
  }

  def logAppTokenRequestForView(viewUid:ViewUid, token:AppToken): Unit = {
    create(MetricIdParts(Fluff("view-"), viewUid, Fluff("-apps")),
      MetricIdParts(Option(token) match {
        case Some(someToken) => someToken
        case None => Fluff("anon")
      }), 1)
  }

  def logApiQueryForView(viewUid:ViewUid, query:QueryString): Unit = {
    create(MetricIdParts(Fluff("view-"), viewUid,Fluff("-query")),
      MetricIdParts(Option(query) match {
        case Some(someQuery) if someQuery != "" => someQuery
        case _ => Fluff("select *")
      }), 1)
    create(viewUid, Fluff("queries-served"), 1)
  }

  def logSocrataAppTokenUsed(ip:Ip): Unit = {
    // TODO? Shouldn't this have an implementation?
  }

  def logMapCreated(domainId:DomainId, parentUid:ViewUid): Unit = {
    create(domainId, Fluff("maps-created"), 1)
    create(parentUid, Fluff("maps-created"), 1)
  }

  def logMapDeleted(domainId:DomainId, parentViewUid:ViewUid): Unit = {
    create(domainId, Fluff("maps-deleted"), 1)
    create(parentViewUid, Fluff("maps-deleted"), 1)
  }

  def logChartCreated(domainId:DomainId, parentViewUid:ViewUid): Unit = {
    create(domainId, Fluff("charts-created"), 1)
    create(parentViewUid, Fluff("charts-created"), 1)
  }

  def logChartDeleted(domainId:DomainId, parentViewUid:ViewUid): Unit = {
    create(domainId, Fluff("charts-deleted"), 1)
    create(parentViewUid, Fluff("charts-deleted"), 1)
  }

  def logFilteredViewCreated(domainId:DomainId, parentViewUid:ViewUid): Unit = {
    create(domainId, Fluff("filters-created"), 1)
    create(parentViewUid, Fluff("filters-created"), 1)
  }

  def logFilteredViewDeleted(domainId:DomainId, parentViewUid:ViewUid): Unit = {
    create(domainId, Fluff("filters-deleted"), 1)
    create(parentViewUid, Fluff("filters-deleted"), 1)
  }

  def logDatasetCreated(domainId:DomainId, viewUid:ViewUid, isBlob: Boolean, isHref: Boolean): Unit = {
    if (isBlob) {
      create(domainId, Fluff("datasets-created-blobby"), 1)
    }
    if (isHref) {
      create(domainId, Fluff("datasets-created-href"), 1)
    }
    create(domainId, Fluff("datasets-created"), 1)
  }

  def logDatasetDeleted(domainId:DomainId, viewUid:ViewUid, isBlob: Boolean, isHref: Boolean): Unit = {
    if (isBlob) {
      create(domainId, Fluff("datasets-deleted-blobby"), 1)
    }
    if (isHref) {
      create(domainId, Fluff("datasets-deleted-href"), 1)
    }
    create(domainId, Fluff("datasets-deleted"), 1)
  }

  def logRowsCreated(count: Int, domainId:DomainId, viewUid:ViewUid, token:AppToken): Unit = {
    create(domainId, Fluff("rows-created"), count)
    create(viewUid, Fluff("rows-created"), count)
  }

  def logRowsDeleted(count: Int, domainId:DomainId, viewUid:ViewUid, token:AppToken): Unit = {
    create(domainId, Fluff("rows-deleted"), count)
    create(viewUid, Fluff("rows-deleted"), count)
    logAppTokenOnView(viewUid, token)
  }

  def logDatasetReferrer(referrer:ReferrerUri, viewUid:ViewUid): Unit = {
    create(MetricIdParts(Fluff("referrer-hosts-"), viewUid),
      MetricIdParts(Fluff("referrer-"), Host(referrer.getHost)), 1)

    create(MetricIdParts(Fluff("referrer-paths-"), viewUid, Fluff("-"), Host(referrer.getHost)),
      MetricIdParts(Fluff("path-"), Path(referrer.getPath)), 1)
  }

  def logPublish(referrer:ReferrerUri, viewUid:ViewUid, domainId:DomainId): Unit = {
    create(MetricIdParts(Fluff("publishes-uids-"), domainId),MetricIdParts(Fluff("uid-"), viewUid), 1)

    create(MetricIdParts(Fluff("publishes-hosts-"), viewUid),
      MetricIdParts(Fluff("referrer-"), Host(referrer.getHost)), 1)

    create(MetricIdParts(Fluff("publishes-paths-"), viewUid, Fluff("-"), Host(referrer.getHost)),
      MetricIdParts(Fluff("path-"), Path(referrer.getPath)), 1)

    create(MetricIdParts(Fluff("publishes-hosts-"), domainId),
      MetricIdParts(Fluff("referrer-"), Host(referrer.getHost)), 1)

    create(MetricIdParts(Fluff("publishes-paths-"), domainId , Fluff("-"), Host(referrer.getHost)),
      MetricIdParts(Fluff("path-"), Path(referrer.getPath)), 1)
  }

  // (actionType: com.socrata.metrics.MetricQueue.Action.Value, viewUid: com.socrata.metrics.ViewUid,
  // domainId: com.socrata.metrics.DomainId, value: Integer, tokenUid: com.socrata.metrics.AppToken) Unit
  def logAction(t:MetricQueue.Action.Value,
                viewUid:ViewUid,
                domainId:DomainId,
                value:Int=1,
                tokenUid:AppToken): Unit = {
    if (t eq Action.RATING) {
      create(viewUid, Fluff("ratings-total"), value)
      create(viewUid, Fluff("ratings-count"), 1)
      create(domainId, Fluff("ratings-total"), value)
      create(domainId, Fluff("ratings-count"), 1)
    } else if (t eq Action.VIEW) {
      create(viewUid, Fluff("view-loaded"), 1)
      create(domainId, Fluff("view-loaded"), 1)
      create(MetricIdParts(Fluff("views-loaded-"), domainId), MetricIdParts(Fluff("view-") , viewUid), 1)
    } else {
      create(viewUid, Fluff(t.toString + "s"), value)
      create(domainId, Fluff(t.toString + "s"), value)
    }
    logAppTokenOnView(viewUid, tokenUid)
  }

  def logBytesInOrOut(inOrOut: String, viewUid:ViewUid, domainId:DomainId, bytes: Long): Unit = {
    if (Option(viewUid).isDefined) {
      create(MetricIdParts(viewUid), MetricIdParts(Fluff("bytes-" + inOrOut)), bytes)
    }
    create(domainId, Fluff("bytes-" + inOrOut), bytes)
  }

  def logRowAccess(accessType:MetricQueue.AccessChannel.Value,
                   domainId:DomainId,
                   viewUid:ViewUid,
                   count: Int,
                   token:AppToken): Unit = {
    create(viewUid, Fluff("rows-accessed-" + accessType), 1)
    create(domainId, Fluff("rows-accessed-" + accessType), 1)
    if (AccessChannel.DOWNLOAD eq accessType) {
      create(MetricIdParts(Fluff("views-downloaded-"), domainId), MetricIdParts(Fluff("view-"), viewUid), 1)
    }
    create(viewUid, Fluff("rows-loaded-" + accessType), count)
    create(domainId, Fluff("rows-loaded-" + accessType), count)
    logAppTokenOnView(viewUid, token)
  }

  def logGeocoding(domainId:DomainId, viewUid:ViewUid, count: Int): Unit = {
    create(viewUid, Fluff("geocoding-requests"), count)
    create(domainId, Fluff("geocoding-requests"), count)
  }

  def logDatasetDiskUsage(timestamp: Long, viewUid:ViewUid, authorUid:UserUid, bytes: Long): Unit = {
    create(viewUid, Fluff("disk-usage"), bytes, timestamp, Metric.RecordType.ABSOLUTE)

    create(MetricIdParts(Fluff("user-"), authorUid, Fluff("-views-disk-usage")),
      MetricIdParts(Fluff("view-"), viewUid), bytes, timestamp, Metric.RecordType.ABSOLUTE)
  }

  def logDomainDiskUsage(timestamp: Long, domainId:DomainId, bytes: Long): Unit = {
    create(domainId, Fluff("disk-usage"), bytes, timestamp, Metric.RecordType.ABSOLUTE)
  }

  def logUserDomainDiskUsage(timestamp: Long, userUid:UserUid, domainId:DomainId, bytes: Long): Unit = {
    create(MetricIdParts(domainId, Fluff("-users-disk-usage")),
      MetricIdParts(Fluff("user-"), userUid), bytes, timestamp, Metric.RecordType.ABSOLUTE)
  }

  private def logAppTokenOnView(realViewUid:ViewUid, token:AppToken): Unit = {
    if (Option(token).isDefined) {
      create(MetricIdParts(realViewUid, Fluff("-apps")), MetricIdParts(token), 1)
      create(MetricIdParts(Fluff("app-"), token), MetricIdParts(realViewUid), 1)
    }
  }
}
