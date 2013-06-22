package com.socrata.metrics

import com.socrata.metrics.migrate._
import com.socrata.balboa.metrics.Metric.RecordType
import java.util.{TimeZone, GregorianCalendar, Date}
import com.socrata.balboa.metrics.data.{Period, DateRange, DataStoreFactory}
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import scala.collection.JavaConverters._


class MigrationOperation(_entity:IdParts, _name:IdParts, _t:RecordType) {
  def getEntity =  _entity
  def getName = _name
  def getRecordType = _t
  def hasPart(p:MetricIdPart) = {
    _entity.hasPart(p) || _name.hasPart(p)
  }
  def isChildOf(op:MigrationOperation) = {
    getEntity.getParts != null && getEntity.getParts.exists { e:IdParts => e.toString().contains(op.getName.toString())}
  }
  def isParent() = {
    _name.getParts != null && _name.getParts.size == 1 && _name.isUnresolved()
  }
  def isUnresolved() = {
    _entity.isUnresolved() || _name.isUnresolved()
  }

  def replacePart(in:MetricIdPart, out:MetricIdPart) = {
    MetricOperation(getEntity.replacePart(in, out), getName.replacePart(in, out), getRecordType)
  }
}
case class MetricOperation(entity:IdParts, name:IdParts, t: RecordType) extends MigrationOperation(entity, name, t)
case class ResolvedMetricOperation(entity:IdParts, name:IdParts, t: RecordType) extends MigrationOperation(entity, name, t)
case class ParentMetricOperation(entity:IdParts, name:IdParts, t: RecordType, children:Seq[MetricOperation]) extends MigrationOperation(entity, name, t)

/** Read one fully resolved metric and write it to another metric **/
case class ReadWriteOperation(read:MigrationOperation, write:MigrationOperation, value:Option[Long]) extends MigrationOperation(read.getEntity, read.getName, read.getRecordType)
/** Read one fully resolved /entity/ for all the metric names and produce ReadWriteOperations for the children **/
case class ReadChildrenOperation(parent:ParentMetricOperation) extends MigrationOperation(parent.entity, parent.name, parent.t)

object Migrator {
  var log:Log = LogFactory.getLog("Migrator")
  def syncViewMetrics(viewUid:ViewUid, destViewUid:ViewUid, domainId:DomainId, start:Date, end:Date) = {
       // Record the operations associated with all log*(viewUid) calls
       log.info("======== Recording ========")
       val recording = new MetricRecord().recordViews(viewUid, domainId)
       recording.foreach(log.trace(_))
       // Convert the recording into a list of Migration Read* operations
       log.info("======== Pass One ======== ")
       val passOne = recording flatMap { op:MigrationOperation => new ResolvedMetricToReadWrite(new ViewUidMetricTransform(viewUid, destViewUid)).apply(op) }
       passOne.foreach(log.trace(_))
       // transform all ReadChildrenOperations into ReadWrite by actually reading the parent entity/metric
       log.info("======== Pass Two ========")
       val passTwo = passOne flatMap { op:MigrationOperation => new ResolveChildrenToReadWrite(DataStoreFactory.get(), start, end).apply(op) }
       passTwo.foreach(log.trace(_))
       // Convert the operations, once more into read/write operations using the specified transform
       log.info("======== Pass Three ========")
       val passThree = passTwo flatMap { op:MigrationOperation => new ResolvedMetricToReadWrite(new ViewUidMetricTransform(viewUid, destViewUid)).apply(op) }
       passThree.foreach(log.trace(_))
       // For each time granularity, compare the read value to the write value and store the difference
       log.info("======== Calculating Deltas ========")
       val dates = new DateRange(start, end).toDates(Period.MONTHLY).asScala

       val deltas = dates flatMap {
         d:Date => {
            val transformer = new CalculatedMetricDelta(DataStoreFactory.get(), Period.MONTHLY, d)
            passThree flatMap {
              op:MigrationOperation => {
                 transformer.apply(op)
              }
            }
         }
       }
       deltas
   }

   def main(args: Array[String]) {
      val start = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
      start.set(2013, 1, 1, 0, 0, 0)
      val end = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
      end.set(2013, 7, 1, 0, 0, 0)

      val stuff = syncViewMetrics(ViewUid(args(0)), ViewUid(args(1)), DomainId(args(2).toInt), start.getTime, end.getTime)
     stuff.foreach {
       m => println(m)
     }
   }
}


