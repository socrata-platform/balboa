package com.socrata.metrics.migrate

import com.socrata.metrics._
import com.socrata.metrics.MetricQueue.{AccessChannel, Action}
import java.util.UUID
import com.socrata.metrics.DomainId
import com.socrata.metrics.AppToken
import com.socrata.metrics.MetricOperation
import com.socrata.metrics.ParentMetricOperation
import com.socrata.metrics.ViewUid

/**
 *
 */
class MetricRecord() {


  def getAppTokenStandIn = AppToken("%appToken:" + UUID.randomUUID().toString + "%")
  def getViewStandIn = ViewUid("%view:" + UUID.randomUUID().toString + "%")
  def getQueryStandIn = QueryString("%query:" + UUID.randomUUID().toString + "%")
  def getReferrerStandIn = ReferrerUri("%referrer:" + UUID.randomUUID().toString + "%")
  def getUserUidStandIn = UserUid("%user:" + UUID.randomUUID().toString + "%")

  def recordViews(uid: ViewUid, domainId: DomainId): Seq[MigrationOperation] = {
    val queue = new MetricWiretapQueue()
    val at = getAppTokenStandIn
    val vs = getViewStandIn
    val qs = getQueryStandIn
    val ref = getReferrerStandIn
    val user = getUserUidStandIn
    queue.logAction(MetricQueue.Action.COMMENT, uid, domainId: DomainId, 1, at)
    queue.logAction(MetricQueue.Action.FAVORITE, uid, domainId: DomainId, 1, at)
    queue.logAction(MetricQueue.Action.RATING, uid, domainId: DomainId, 1, at)
    queue.logAction(MetricQueue.Action.SHARE, uid, domainId: DomainId, 1, at)
    queue.logAction(MetricQueue.Action.VIEW, uid, domainId: DomainId, 1, at)
    queue.logViewChildLoaded(uid: ViewUid, vs, null)
    queue.logViewChildLoaded(uid: ViewUid, vs, "map")
    queue.logViewChildLoaded(uid: ViewUid, vs, "chart")
    queue.logViewSearch(uid, qs)
    queue.logAppTokenRequestForView(uid, at)
    queue.logApiQueryForView(uid, qs)
    queue.logMapCreated(domainId, uid)
    queue.logMapDeleted(domainId, uid)
    queue.logChartCreated(domainId, uid)
    queue.logChartDeleted(domainId, uid)
    queue.logFilteredViewCreated(domainId, uid)
    queue.logFilteredViewDeleted(domainId, uid)
    queue.logRowsCreated(1, domainId, uid, at)
    queue.logRowsDeleted(1, domainId, uid, at)
    queue.logDatasetReferrer(ref, uid)
    queue.logPublish(ref, uid, domainId)
    queue.logBytesInOrOut("in", uid, domainId, 1)
    queue.logBytesInOrOut("out", uid, domainId, 1)
    queue.logRowAccess(AccessChannel.API, domainId, uid, 1, at)
    queue.logRowAccess(AccessChannel.DOWNLOAD, domainId, uid, 1, at)
    queue.logRowAccess(AccessChannel.EMAIL, domainId, uid, 1, at)
    queue.logRowAccess(AccessChannel.PRINT, domainId, uid, 1, at)
    queue.logRowAccess(AccessChannel.RSS, domainId, uid, 1, at)
    queue.logRowAccess(AccessChannel.UNKNOWN, domainId, uid, 1, at)
    queue.logRowAccess(AccessChannel.WEBSITE, domainId, uid, 1, at)
    queue.logRowAccess(AccessChannel.WIDGET, domainId, uid, 1, at)
    queue.logGeocoding(domainId, uid, 1)
    dedupe(filterByView(uid, resolveDependent(queue.record())))
  }

  def filterByView(part:MetricIdPart, ops:Seq[MigrationOperation]):Seq[MigrationOperation] = {
    ops.filter {
      op:MigrationOperation => op.hasPart(part)
    }
  }

  def dedupe(ops:Seq[MigrationOperation]):Seq[MigrationOperation] = {
     ops.distinct
  }

  def resolveDependent(raw:Seq[MetricOperation]):Seq[MigrationOperation] = {
      // search for any ops where the metric name is entirely on the stand-in list
      // make these parents
      // search for any ops where the metric name contains a string on the stand-in-list
      // these are the children of those parents
      val standalone = raw.filter {
        m:MetricOperation => {
          !m.isUnresolved()
        }
      }.map {
        p:MetricOperation => ResolvedMetricOperation(p.entity, p.name, p.t)
      }

      val parents = raw.filter {
         m:MetricOperation => m.isParent()
      }.map {
        p:MetricOperation => ParentMetricOperation(p.entity, p.name, p.t, raw.filter {
          m:MetricOperation => {
            m.isChildOf(p)
          }
        }.distinct)
      }

      parents ++ standalone

  }

}
