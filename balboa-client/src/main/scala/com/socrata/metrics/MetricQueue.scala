package com.socrata.metrics

import com.socrata.balboa.metrics.Metric




object MetricQueue {
  val AGGREGATE_GRANULARITY: Long = 120 * 1000

  final object Action extends Enumeration {
    type Action = Value
    val SHARE = Value("share")
    val COMMENT = Value("comment")
    val RATING = Value("rating")
    val VIEW = Value("view")
    val FAVORITE = Value("favorite")
  }

  final object Import extends Enumeration {
    type Import = Value
    val REPLACE = Value("replace")
    val IMPORT = Value("import")
    val APPEND = Value("append")
  }

  final object AccessChannel extends Enumeration {
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

abstract trait MetricQueue {

  def create(entity: IdParts, name: IdParts, value: Long, timestamp: Long, recordType: Metric.RecordType)

  def create(entity: IdParts, name: IdParts, value: Long, timestamp: Long)

  def create(entity: IdParts, name: IdParts, value: Long)

  def logViewChildLoaded(parentViewUid:ViewUid, childViewUid:ViewUid, displaytype: String)

  def logViewSearch(viewUid:ViewUid, query:QueryString)

  def logUserSearch(domainId:DomainId, query:QueryString)

  def logDatasetSearch(domainId:DomainId, query:QueryString)

  def logUserCreated(domainId:DomainId)

  def logFilteredViewCreated(domainId:DomainId, parentViewUid: ViewUid)

  def logFilteredViewDeleted(domainId:DomainId, parentViewUid:ViewUid)

  def logDatasetCreated(domainId:DomainId, viewUid:ViewUid, isBlob: Boolean, isHref: Boolean)

  def logDatasetDeleted(domainId:DomainId, viewUid:ViewUid, isBlob: Boolean, isHref: Boolean)

  def logMapCreated(domainId:DomainId, parentUid:ViewUid)

  def logMapDeleted(domainId:DomainId, parentViewUid:ViewUid)

  def logChartCreated(domainId:DomainId, parentViewUid:ViewUid)

  def logChartDeleted(domainId:DomainId, parentViewUid:ViewUid)

  def logRowsCreated(count: Int, domainId:DomainId, viewUid:ViewUid, appTokenId:AppToken)

  def logRowsDeleted(count: Int, domainId:DomainId, viewUid:ViewUid, appTokenUid:AppToken)

  def logPublish(url:ReferrerUri, viewUid:ViewUid, domainId:DomainId)

  def logDatasetReferrer(url:ReferrerUri, viewUid:ViewUid)

  def logAction(actionType:MetricQueue.Action.Value, viewUid:ViewUid, domainId:DomainId, value:Int, tokenUid:AppToken)

  def logRowAccess(accessChannel:MetricQueue.AccessChannel.Value, domainId:DomainId, viewUid:ViewUid, count:Int, appTokenUid:AppToken)

  def logBytesInOrOut(inOrOut: String, viewUid:ViewUid, domainId:DomainId, bytes: Long)

  def logGeocoding(domainId:DomainId, viewUid:ViewUid, count: Int)

  def logDatasetDiskUsage(timestamp: Long, viewUid:ViewUid, authorUid:UserUid, bytes: Long)

  def logDomainDiskUsage(timestamp: Long, domainId:DomainId, bytes: Long)

  def logUserDomainDiskUsage(timestamp: Long, userUid:UserUid, domainId:DomainId, bytes: Long)

  def logAppTokenCreated(domainId:DomainId)

  def logAppTokenRequest(tokenUid:AppToken, domainId:DomainId, ip:Ip)

  def logAppTokenRequestForView(viewUid:ViewUid, token:AppToken)

  def logApiQueryForView(viewUid:ViewUid, query:QueryString)

  def logSocrataAppTokenUsed(ip:Ip)
}