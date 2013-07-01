package com.socrata.metrics.migrate

import com.socrata.metrics._
import com.socrata.metrics.ReadWriteOperation
import com.socrata.metrics.ViewUid
import com.socrata.balboa.metrics.data.{DataStoreFactory, DateRange, Period, DataStore}
import java.util.Date
import scala.collection.JavaConverters._
import com.socrata.balboa.metrics.{Metric, Metrics}
import java.util
import com.socrata.balboa.metrics.Metric.RecordType
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import java.util.concurrent.ConcurrentHashMap

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

object CalculatedMetricDelta {
  val metricCache = new ConcurrentHashMap[String,util.Iterator[Metrics]]()
}

class CalculatedMetricDelta(ds: DataStore, period:Period, time:Date) extends MetricTransform {

  def getValue(entityMetrics:util.Iterator[Metrics], metricName:String):Long = {
    if (entityMetrics == null || !entityMetrics.hasNext) {
      return 0L
    }
    val metrics = entityMetrics.next().get(metricName)
    if (metrics == null) {
      return 0L
    }
    metrics.getValue.longValue()
  }

  def apply(op: MigrationOperation) = {
    log.info("            Calculating metric delta for instance " + time + " period " + period + " metric: " + op.getEntity + ":" + op.getName)
    op match {
      case rw:ReadWriteOperation => {
        val readMetric = rw.read
        val writeMetric = rw.write
        val cacheKey = readMetric.getEntity.toString() + "-" + period + "-" + time.getTime
        val cachedEntity = CalculatedMetricDelta.metricCache.get(cacheKey)
        val readEntity = if (cachedEntity != null) cachedEntity
          else {
            val entry = ds.find(readMetric.getEntity.toString(), period, time);
            CalculatedMetricDelta.metricCache.put(cacheKey, entry);
            entry
          }
        val readValue = getValue(readEntity, readMetric.getName.toString())
        val writeEntity = ds.find(writeMetric.getEntity.toString(), period, time)
        val writeValue = getValue(writeEntity, writeMetric.getName.toString())
        if ((readValue - writeValue) > 0)
          Seq(ReadWriteOperation(readMetric, writeMetric, Some(readValue - writeValue), time, period))
        else
          Seq()
      }
      case op:MigrationOperation => throw new InternalError("Delta calculations can only be performed on RW pairs")
    }
  }
}

class ExpandToRange(period:Period, start:Date, end:Date) extends MetricTransform {
  def apply(op: MigrationOperation) = {
    new DateRange(start, end).toDates(period).asScala flatMap {
      d:Date => new CalculatedMetricDelta(DataStoreFactory.get(), period, d).apply(op)
    }
  }
}

class ExpandOpToPeriod(period:Period) extends MetricTransform {
  def apply(op: MigrationOperation) = {
    log.info("Calculating sub-periods for instance metric: " + op.getEntity + ":" + op.getName)
    op match {
      case p: ReadWriteOperation => {
        log.info("      Expanding metric from " + p.period + " to " + period + " values " + p.getValue())
        assert(p.period.compareTo(period) < 0)
        if (p.getValue().isDefined && p.getValue().get > 0) {
          val fullRange = DateRange.create(p.period, p.time)
          new ExpandToRange(period, fullRange.start, fullRange.end).apply(op)
        } else {
          log.info("      Ignoring zeroed out metric: " + p.getEntity + ":" + p.getName)
          Seq()
        }
      }
      case _:MigrationOperation => assert(false); Seq()
    }
  }
}

class WriteMetric(ds: DataStore, dryrun:Boolean) extends MetricTransform {
  def apply(op: MigrationOperation) = {
    op match {
      case p: ReadWriteOperation => {
          //assert(p.period == Period.FIFTEEN_MINUTE)
          assert(p.getValue().get > 0)
          log.info("Writing metric " + p.write.getEntity + ":" + p.write.getName + ":" + p.write.getRecordType + " -> " + p.getValue().get)
          if (!dryrun) {
            val metrics = new Metrics()
            val metric = new Metric()
            metric.setType(p.write.getRecordType)
            metric.setValue(p.getValue().get)
            metrics.put(p.write.getName.toString(), metric)
            ds.persist(p.write.getEntity.toString(), p.getTime().getTime, metrics)
          }
          Seq()
      }
      case _:MigrationOperation => assert(false); Seq()
    }
  }
}

