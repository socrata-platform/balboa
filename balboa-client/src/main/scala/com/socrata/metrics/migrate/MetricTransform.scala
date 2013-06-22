package com.socrata.metrics.migrate

import com.socrata.metrics._
import com.socrata.metrics.ReadWriteOperation
import com.socrata.metrics.ViewUid
import com.socrata.balboa.metrics.data.DataStore
import java.util.Date
import scala.collection.JavaConverters._
import com.socrata.balboa.metrics.Metrics
import java.util
import com.socrata.balboa.metrics.Metric.RecordType
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory

/**
 *
 */
trait MetricTransform {
  var log:Log = LogFactory.getLog(classOf[MetricTransform])
  def apply(op:MigrationOperation):Seq[MigrationOperation]
}

class ViewUidMetricTransform(in:ViewUid, out:ViewUid) extends MetricTransform {
  def apply(op:MigrationOperation) = {
    log.info("View Transform " + op.getEntity + ":" + op.getName)
    val transformed = op.replacePart(in, out)
    log.info("   Transformed to " + transformed.getEntity + ":" + transformed.getName)
    Seq(transformed)
  }
}

class ResolvedMetricToReadWrite(transform:MetricTransform) extends MetricTransform {
  def apply(op: MigrationOperation) = {
    log.info("Resolving Metric RW " + op.getEntity + ":" + op.getName)
    op match {
      case resolved:ResolvedMetricOperation => transform.apply(resolved).map(ReadWriteOperation(resolved, _, None))
      case parent:ParentMetricOperation => {
        val tr = transform.apply(parent)
        tr.map { _:MigrationOperation => ReadChildrenOperation(parent) }
      }
      case x:MigrationOperation => Seq[MigrationOperation](x)
    }
  }
}


class ResolveChildrenToReadWrite(ds: DataStore, start: Date, end: Date) extends MetricTransform {
  def parentToOperation(entity:IdParts, name:String, recordType:RecordType) = {
    Seq(ResolvedMetricOperation(entity, Fluff(name), recordType))
  }

  def childrenToOperation(name:String, children:Seq[MigrationOperation]):Seq[MigrationOperation] = {
    children flatMap {
      child:MigrationOperation => {
        log.info("Child to Resolved Op " + child.getEntity + ":" + child.getName + " <- " + name)
        assert(child.isUnresolved())
        assert(!child.getName.isUnresolved())
        Seq(ResolvedMetricOperation(child.getEntity.replaceFirstUnresolved(Fluff(name)), child.getName, child.getRecordType))
      }
    }
  }

  def apply(op: MigrationOperation) = {
    log.info("Resolving Children RW " + op.getEntity + ":" + op.getName)
    op match {
      case p: ReadChildrenOperation => {
        val entity = p.parent.getEntity

        ds.find(entity.toString(), start, end).asScala flatMap {
          m: Metrics => {
            m.keySet().asScala flatMap {
              name:String =>
                val par = parentToOperation(entity, name, p.getRecordType)
                // iterated through children and generate new resolved metrics by replacing the
                // unresolved id part of the new child metric with the
                val child = childrenToOperation(name, p.parent.children)
                par ++ child
            }
          }
        }
      }.toSeq
      case op: MigrationOperation => Seq(op)
    }

  }
}

