package com.socrata.metrics

import com.socrata.metrics.migrate._
import com.socrata.balboa.metrics.Metric.RecordType
import java.util.{TimeZone, GregorianCalendar, Date}
import com.socrata.balboa.metrics.data.{Period, DateRange, DataStoreFactory}
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import scala.collection.JavaConverters._
import com.socrata.balboa.metrics.Metrics
import au.com.bytecode.opencsv.CSVWriter
import java.io.OutputStreamWriter


class MigrationOperation(_entity:IdParts, _name:IdParts, _t:RecordType) {
  def getEntity =  _entity
  def getName = _name
  def getRecordType = _t
  def hasPart(p:MetricIdPart) = {
    _entity.hasPart(p) || _name.hasPart(p)
  }
  def isChildOf(op:MigrationOperation) = {
    getEntity.getParts != null && getEntity.getParts.exists {
      e:IdParts => {
        val childPart = e.toString
        op.getName.getParts.exists {
          parentPart:MetricIdPart => {
            parentPart.isUnresolved() && childPart.equals(parentPart.toString())
          }
        }
      }
    }
  }
  def isParent() = {
    _name.getParts != null && _name.isUnresolved()
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
case class UnresolvedMetricOperation(entity:IdParts, name:IdParts, t: RecordType) extends MigrationOperation(entity, name, t)
case class ParentMetricOperation(entity:IdParts, name:IdParts, t: RecordType, children:Seq[MetricOperation]) extends MigrationOperation(entity, name, t)
case class FullEntityMetricOperation(entity:IdParts, t:RecordType) extends MigrationOperation(entity, Unknown(), t)

/** Read one fully resolved metric and write it to another metric **/
case class ReadWriteOperation(read:MigrationOperation, write:MigrationOperation, value:Option[Long], time:Date = new Date(), period:Period = Period.FIFTEEN_MINUTE) extends MigrationOperation(read.getEntity, read.getName, read.getRecordType) {
  def getValue():Option[Long] =  value
  def getTime() = time
}
/** Read one fully resolved /entity/ for all the metric names and produce ReadWriteOperations for the children **/
case class ReadChildrenOperation(parent:ParentMetricOperation) extends MigrationOperation(parent.entity, parent.name, parent.t)


object Migrator {
  var log:Log = LogFactory.getLog("Migrator")

  def expandInTime(ops:Seq[MigrationOperation], start:Date, end:Date, readOnly:Boolean) = {
    log.info("======== Calculating Yearly ========")
    val yearlies = ops flatMap {
      y:MigrationOperation => new ExpandToRange(Period.YEARLY, start, end, readOnly).apply(y)
    }
    log.info("======== Calculating Monthly ========")
    val monthlies = yearlies flatMap {
      y:MigrationOperation => new ExpandOpToPeriod(Period.MONTHLY, readOnly).apply(y)
    }

    log.info("======== Calculating Daily ========")
    val dailies = monthlies flatMap {
      y:MigrationOperation => new ExpandOpToPeriod(Period.DAILY, readOnly).apply(y)
    }
    log.info("======== Calculating Hourly ========")
    val deltas = dailies flatMap {
      y:MigrationOperation => new ExpandOpToPeriod(Period.HOURLY, readOnly).apply(y)
    }
    deltas.toList
  }

  def migrate(ops:Seq[MigrationOperation], start:Date, end:Date, dryrun:Boolean) = {
    val deltas = expandInTime(ops, start, end, false)
    log.info("======== Writing Metrics ========")
    deltas flatMap {
      d:MigrationOperation => new WriteMetric(DataStoreFactory.get(), dryrun).apply(d)
    }
  }

  def toCSV(ops:Seq[MigrationOperation], writer:CSVWriter) = {
    log.info("======== Writing Metrics ========")
    log.info(ops.size + " things to write")
    ops map {
      case d:ReadWriteOperation => writer.writeNext(Array(
        d.write.getEntity.toString,
        d.getTime().getTime.toString,
        d.write.getName.toString,
        d.getRecordType.toString,
        d.getValue.get.toString
      ))
      case op:MigrationOperation => log.error("I can't write " + op)
    }
  }

  def preprocess(ops:Seq[MigrationOperation],start:Date, end:Date, transform:MetricTransform) = {
    // Convert the recording into a list of Migration Read* operations
    log.info("======== Pass One ======== ")
    val passOne = ops flatMap { op:MigrationOperation => new ResolvedMetricToReadWrite(transform).apply(op) }
    passOne.foreach(log.trace(_))
    // transform all ReadChildrenOperations into ReadWrite by actually reading the parent entity/metric
    log.info("======== Pass Two ========")
    val passTwo = passOne flatMap { op:MigrationOperation => new ResolveChildrenToReadWrite(DataStoreFactory.get(), start, end).apply(op) }
    passTwo.foreach(log.trace(_))
    // Convert the operations, once more into read/write operations using the specified transform
    log.info("======== Pass Three ========")
    val passThree = passTwo flatMap { op:MigrationOperation => new ResolvedMetricToReadWrite(transform).apply(op) }
    passThree.foreach(log.trace(_))
    passThree
  }

  def syncViewMetrics(viewUid:ViewUid, destViewUid:ViewUid, domainId:DomainId, start:Date, end:Date, dryrun:Boolean) = {
       // Record the operations associated with all log*(viewUid) calls
       log.info("======== Recording ========")
       val recording = new MetricRecord().recordViews(viewUid, domainId)
       recording.foreach(log.trace(_))
       val rw = preprocess(recording, start, end, new MetricPartTransform(viewUid, destViewUid))
       migrate(rw, start, end, dryrun)
   }

   // copy 4x4/rows-accessed-download to 4x4/files-downloaded
   def esriToMondara(viewUid:ViewUid, start:Date, end:Date, dryrun:Boolean) = {
     val from = new ResolvedMetricOperation(viewUid, Fluff("rows-accessed-download"), RecordType.AGGREGATE)
     val to= new ResolvedMetricOperation(viewUid, Fluff("files-downloaded"), RecordType.AGGREGATE)
     val rw = ReadWriteOperation(from, to, None)
     migrate(Seq(rw), start, end, dryrun)
   }

   def viewsLoadedToViews(id:DomainId, start:Date, end:Date):Iterator[String] = {
     DataStoreFactory.get().find("views-loaded-" + id.toString(), start, end).asScala flatMap {
       m: Metrics =>
         m.keySet().asScala flatMap {
           name:String => Seq(name.replaceFirst("view-", ""))
         }
     }
   }

   def dumpDomain(src:DomainId, dest:DomainId, start:Date, end:Date) {
     val site = Seq(
       new FullEntityMetricOperation(src, RecordType.AGGREGATE),
       new FullEntityMetricOperation(MetricIdParts(src, Fluff("-applications")), RecordType.AGGREGATE),
       new FullEntityMetricOperation(MetricIdParts(src, Fluff("-intern")), RecordType.AGGREGATE),
       new FullEntityMetricOperation(MetricIdParts(Fluff("views-loaded-"), src), RecordType.AGGREGATE),
       new FullEntityMetricOperation(MetricIdParts(Fluff("views-downloaded-"), src), RecordType.AGGREGATE)
     )
     //val viewUIDs = viewsLoadedToViews(src, start, end)
     //val viewMetrics = viewUIDs flatMap {
     //    uid:String => new MetricRecord().recordViews(ViewUid(uid), src)
     //}
     //val initial = site ++ viewMetrics
     val initial = site
     log.info("Initial Set:")
     initial.foreach(log.info(_))
     Thread.sleep(10000)
     val one = preprocess(initial, start, end, new MetricPartTransform(src, dest))
     log.info("Post Processed:")
     one.foreach(log.info(_))
     Thread.sleep(10000)
     val deltas = expandInTime(one, start, end, true)
     val writer:CSVWriter = new CSVWriter(new OutputStreamWriter(System.out, "UTF-8"), ',', '"');
     log.info("Initial Set Count: " + initial.size)
     log.info("Post Processed Count: " + one.size)
     log.info("Deltas Count: " + deltas.size)
     toCSV(deltas, writer)
     writer.flush()
     writer.close()

   }

   def main(args: Array[String]) {
      val start = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
      start.set(2009, 1, 1, 0, 0, 0)
      val end = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
      end.set(2014, 1, 1, 0, 0, 0)
      val cmd = if (args.length > 1) args(0) else "help"
      cmd.toString match {
        case "view" => syncViewMetrics(ViewUid(args(1)), ViewUid(args(2)), DomainId(args(3).toInt), start.getTime, end.getTime, !"real".equals(args(4)))
        case "esri2Mondara" => esriToMondara(ViewUid(args(1)), start.getTime, end.getTime, !"real".equals(args(2)))
        case "dump-domain" => dumpDomain(DomainId(args(1).toInt), DomainId(args(2).toInt), start.getTime, end.getTime)
        case "help" =>  {
          println("Migrator view [src view 4x4] [dst view 4x4] [domain id] [dryrun|real]")
          println("Migrator esri2Mondara view 4x4] [dryrun|real]")
          println("Migrator dump-domain domainId destinationDomainId")
          Seq()
        }
      }
   }
}


