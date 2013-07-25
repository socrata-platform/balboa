package com.socrata.metrics.migrate

import com.socrata.metrics._
import com.socrata.metrics.ReadWriteOperation
import com.socrata.metrics.ViewUid
import com.socrata.balboa.metrics.data.{DataStoreFactory, DateRange, Period, DataStore}
import java.util.{Collections, Date}
import scala.collection.JavaConverters._
import com.socrata.balboa.metrics.{Timeslice, Metric, Metrics}
import java.util
import com.socrata.balboa.metrics.Metric.RecordType
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.commons.collections.map.LRUMap

/**
 *
 */
trait MetricTransform {
  var log:Log = LogFactory.getLog(classOf[MetricTransform])
  def apply(op:MigrationOperation):Seq[MigrationOperation]
}

class MetricPartTransform(in:MetricIdPart, out:MetricIdPart) extends MetricTransform {
  def apply(op:MigrationOperation) = {
    log.info("Metric Part Transform " + op.getEntity + ":" + op.getName)
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
      case full:FullEntityMetricOperation => {
        ds.find(full.entity.toString(), start, end).asScala flatMap {
          m: Metrics => {
            m.keySet().asScala flatMap {
              name:String => Seq(ResolvedMetricOperation(full.entity, Fluff(name), m.get(name).getType))
            }
          }
        }
      }.toSeq
      case op: MigrationOperation => Seq(op)
    }

  }
}

object CalculatedMetricDelta {
  val lruRead = Collections.synchronizedMap(new LRUMap(1024).asInstanceOf[java.util.Map[String, Metrics]])
  val lruWrite = Collections.synchronizedMap(new LRUMap(1024).asInstanceOf[java.util.Map[String, Metrics]])
  @volatile var hits = 0
  @volatile var misses = 0

}

class CalculatedMetricDelta(readDataStore:DataStore, writeDataStore:DataStore, period:Period, time:Date) extends MetricTransform {

  def getValue(entityMetrics:Metrics, metricName:String):Long = {
    if (entityMetrics == null) {
      return 0L
    }
    val metrics = entityMetrics.get(metricName)
    if (metrics == null) {
      return 0L
    }
    metrics.getValue.longValue()
  }

  def getEntry(op:MigrationOperation, cache:java.util.Map[String, Metrics], ds:DataStore) = {
    val cacheKey = op.getEntity.toString() + "-" + period + "-" + time.getTime
    val cachedEntity = cache.get(cacheKey)
    if (cachedEntity != null) {
      CalculatedMetricDelta.hits += 1
      cachedEntity
    } else {
      CalculatedMetricDelta.misses += 1
      val entry = ds.find(op.getEntity.toString(), period, time).next
      cache.put(cacheKey, entry)
      entry
    }
  }

  def apply(op: MigrationOperation) = {
    op match {
      case rw:ReadWriteOperation => {
        val readMetric = rw.read
        val writeMetric = rw.write
        val readEntity = getEntry(readMetric, CalculatedMetricDelta.lruRead, readDataStore)
        val readValue = getValue(readEntity, readMetric.getName.toString())
        var writeValue = 0L
        if (writeDataStore != null && !rw.isDestZero()) {
           val writeEntity = getEntry(writeMetric, CalculatedMetricDelta.lruWrite, writeDataStore)
           writeValue = getValue(writeEntity, writeMetric.getName.toString())
        }
        if ((readValue - writeValue) != 0) {
          log.info("            Found metric delta for instance " + time + " period " + period + " metric: " + op.getEntity + ":" + op.getName + " delta:" + (readValue - writeValue) + " read: " + readValue + " write: " + writeValue + " hits: " + CalculatedMetricDelta.hits + " miss: " + CalculatedMetricDelta.misses)
          Seq(ReadWriteOperation(readMetric, writeMetric, Some(readValue - writeValue), time, period, writeValue == 0))
        } else
          Seq()
      }
      case op:MigrationOperation => throw new InternalError("Delta calculations can only be performed on RW pairs")
    }
  }
}

object ExpandToRange {
  val lruSlices = Collections.synchronizedMap(new LRUMap(1024).asInstanceOf[java.util.Map[String, List[Timeslice]]])
  @volatile var hits = 0
  @volatile var misses = 0

}

class ExpandToRange(readDataStore:DataStore, writeDataStore:DataStore, period:Period, start:Date, end:Date) extends MetricTransform {
  def apply(op: MigrationOperation) = {
    op match {
      case rw:ReadWriteOperation => {
          if (rw.isDestZero()) {
            // The write side of this is zeroed out, dump the metrics directly from a single slice operation
            val cacheKey = op.getEntity.toString() + "-" + period + "-" + start.getTime
            val readSlice = if (ExpandToRange.lruSlices.containsKey(cacheKey)) {
              ExpandToRange.hits += 1
              ExpandToRange.lruSlices.get(cacheKey)
            } else {
              ExpandToRange.misses += 1
              val slice = readDataStore.slices(op.getEntity.toString(), period, start, end).asScala.toList
              ExpandToRange.lruSlices.put(cacheKey, slice)
              slice
            }
            readSlice flatMap {
              ts:Timeslice => {
                val metric = ts.getMetrics.get(op.getName.toString())
                if (metric != null && ts.getStart >= start.getTime && ts.getEnd <= end.getTime) {
                  log.info("            Using metric slice to expand metrics value:" + metric.getValue.longValue() + " date: " + new Date(ts.getStart) + " hits: " + ExpandToRange.hits + " misses: " + ExpandToRange.misses)
                  Seq(ReadWriteOperation(rw.read, rw.write, Some(metric.getValue.longValue()), new Date(ts.getStart), period, true))
                } else {
                  Seq()
                }

              }
            }
          } else {
            // calculate deltas for each individual timerange
            new DateRange(start, end).toDates(period).asScala flatMap {
              d:Date => new CalculatedMetricDelta(readDataStore, writeDataStore, period, d).apply(op)
            }
          }
      }
      case _:MigrationOperation => throw new InternalError("Time range expansion can only be performed on RW pairs")
    }
  }
}

class ExpandOpToPeriod(readDataStore:DataStore, writeDataStore:DataStore, period:Period) extends MetricTransform {
  def apply(op: MigrationOperation) = {
    log.info("Calculating sub-periods for instance metric: " + op.getEntity + ":" + op.getName)
    op match {
      case p: ReadWriteOperation => {
        log.info("      Expanding metric from " + p.period + " to " + period + " values " + p.getValue())
        assert(p.period.compareTo(period) < 0)
        if (p.getValue().isDefined && p.getValue().get != 0) {
          val fullRange = DateRange.create(p.period, p.time)
          new ExpandToRange(readDataStore, writeDataStore, period, fullRange.start, fullRange.end).apply(op)
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
          assert(p.getValue().get != 0)
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

