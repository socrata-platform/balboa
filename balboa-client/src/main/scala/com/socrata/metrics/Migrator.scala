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
case class ReadWriteOperation(read:MigrationOperation, write:MigrationOperation, value:Option[Long], time:Date = new Date(), period:Period = Period.FIFTEEN_MINUTE) extends MigrationOperation(read.getEntity, read.getName, read.getRecordType) {
  def getValue():Option[Long] =  value
  def getTime() = time
}
/** Read one fully resolved /entity/ for all the metric names and produce ReadWriteOperations for the children **/
case class ReadChildrenOperation(parent:ParentMetricOperation) extends MigrationOperation(parent.entity, parent.name, parent.t)


object Migrator {
  var log:Log = LogFactory.getLog("Migrator")

  def migrate(ops:Seq[MigrationOperation], start:Date, end:Date, dryrun:Boolean) = {
    log.info("======== Calculating Yearly ========")
    val yearlies = ops flatMap {
      y:MigrationOperation => new ExpandToRange(Period.YEARLY, start, end).apply(y)
    }
    log.info("======== Calculating Monthly ========")
    val monthlies = yearlies.par flatMap {
      y:MigrationOperation => new ExpandOpToPeriod(Period.MONTHLY).apply(y)
    }
    //og.info("======== Calculating Weekly ========")
    //val weeklies = monthlies flatMap {
    //  y:MigrationOperation => new ExpandOpToPeriod(Period.WEEKLY).apply(y)
    //}
    log.info("======== Calculating Daily ========")
    val dailies = monthlies.par flatMap {
      y:MigrationOperation => new ExpandOpToPeriod(Period.DAILY).apply(y)
    }
    log.info("======== Calculating Hourly ========")
    val deltas = dailies.par flatMap {
      y:MigrationOperation => new ExpandOpToPeriod(Period.HOURLY).apply(y)
    }
    //log.info("======== Calculating Fifteen Minutely ========")
    //val deltas = hourlies flatMap {
    //  y:MigrationOperation => new ExpandOpToPeriod(Period.FIFTEEN_MINUTE).apply(y)
    //}
    log.info("======== Writing Metrics ========")

    deltas flatMap {
      d:MigrationOperation => new WriteMetric(DataStoreFactory.get(), dryrun).apply(d)
    }
  }

  def syncViewMetrics(viewUid:ViewUid, destViewUid:ViewUid, domainId:DomainId, start:Date, end:Date, dryrun:Boolean) = {
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
       migrate(passThree, start, end, dryrun)
   }

   // copy 4x4/rows-accessed-download to 4x4/files-downloaded
   def esriToMondara(viewUid:ViewUid, start:Date, end:Date, dryrun:Boolean) = {
     val from = new ResolvedMetricOperation(viewUid, Fluff("rows-accessed-download"), RecordType.AGGREGATE)
     val to= new ResolvedMetricOperation(viewUid, Fluff("files-downloaded"), RecordType.AGGREGATE)
     val rw = ReadWriteOperation(from, to, None)
     migrate(Seq(rw), start, end, dryrun)
   }

   def main(args: Array[String]) {
      val start = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
      start.set(2009, 1, 1, 0, 0, 0)
      val end = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
      end.set(2014, 1, 1, 0, 0, 0)
      args(0).toString match {
        case "view" => syncViewMetrics(ViewUid(args(1)), ViewUid(args(2)), DomainId(args(3).toInt), start.getTime, end.getTime, !"real".equals(args(4)))
        case "e2m" => esriToMondara(ViewUid(args(1)), start.getTime, end.getTime, !"real".equals(args(2)))
        case _ =>  {
          println("Migrator view [src view 4x4] [dst view 4x4] [domain id] [dryrun|real]")
          println("Migrator e2m view 4x4] [dryrun|real]")
          Seq()
        }
      }
   }
}


