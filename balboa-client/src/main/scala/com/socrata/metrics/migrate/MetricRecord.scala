package com.socrata.metrics.migrate

import com.socrata.metrics._
import com.socrata.metrics.MetricQueue.Action
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

  var _appTokens = List[AppToken]()

  def getAppTokenStandIn = {
      val at = AppToken("%appToken:" + UUID.randomUUID().toString + "%")
      _appTokens = at :: _appTokens
      at
  }

  def recordViews(uid:ViewUid, domainId:DomainId):Seq[MigrationOperation] = {
      val queue = new MetricWiretapQueue()
      val at = getAppTokenStandIn
      queue.logAction(MetricQueue.Action.COMMENT, uid, domainId:DomainId, 1, at)
      queue.logAction(MetricQueue.Action.FAVORITE, uid, domainId:DomainId, 1, at)
      queue.logAction(MetricQueue.Action.RATING, uid, domainId:DomainId, 1, at)
      queue.logAction(MetricQueue.Action.SHARE, uid, domainId:DomainId, 1, at)
      queue.logAction(MetricQueue.Action.VIEW, uid, domainId:DomainId, 1, at)
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
