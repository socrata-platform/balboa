package com.socrata.metrics

import com.socrata.metrics.migrate._
import com.socrata.balboa.metrics.Metric.RecordType
import java.util.{TimeZone, GregorianCalendar, Date}
import com.socrata.balboa.metrics.data.{DataStore, Period, DateRange, DataStoreFactory}
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import scala.collection.JavaConverters._
import com.socrata.balboa.metrics.Metrics
import au.com.bytecode.opencsv.CSVWriter
import java.io.OutputStreamWriter
import com.socrata.balboa.metrics.config.{PropertiesConfiguration, Configuration}
import java.text.SimpleDateFormat


class MigrationOperation(_entity:IdParts, _name:IdParts, _t:RecordType) extends Ordered[MigrationOperation] {
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

  def compare(that: MigrationOperation) = _entity.toString().compare(that.getEntity.toString())
}
case class MetricOperation(entity:IdParts, name:IdParts, t: RecordType) extends MigrationOperation(entity, name, t)
case class ResolvedMetricOperation(entity:IdParts, name:IdParts, t: RecordType) extends MigrationOperation(entity, name, t)
case class UnresolvedMetricOperation(entity:IdParts, name:IdParts, t: RecordType) extends MigrationOperation(entity, name, t)
case class ParentMetricOperation(entity:IdParts, name:IdParts, t: RecordType, children:Seq[MetricOperation]) extends MigrationOperation(entity, name, t)
case class FullEntityMetricOperation(entity:IdParts, t:RecordType) extends MigrationOperation(entity, Unknown(), t)

/** Read one fully resolved metric and write it to another metric **/
case class ReadWriteOperation(read:MigrationOperation, write:MigrationOperation, value:Option[Long], time:Date = new Date(), period:Period = Period.HOURLY, destIsZero:Boolean = false) extends MigrationOperation(read.getEntity, read.getName, read.getRecordType) {
  def getValue():Option[Long] =  value
  def getTime() = time
  def isDestZero() = destIsZero
  def compare(that: ReadWriteOperation) = getTime().getTime - that.getTime().getTime

}
/** Read one fully resolved /entity/ for all the metric names and produce ReadWriteOperations for the children **/
case class ReadChildrenOperation(parent:ParentMetricOperation) extends MigrationOperation(parent.entity, parent.name, parent.t)


object Migrator {
  var log:Log = LogFactory.getLog("Migrator")

  def expandInPeriod(ops:Seq[MigrationOperation], period:Period, start:Date, end:Date, readDataStore:DataStore, writeDataStore:DataStore) = {
    log.info("======== Calculating " + period + " ========")
    val total =  ops.size
    var pos = 0
    val finer = ops.distinct.toArray.sorted.par flatMap {
      op => op match {
        case y:ReadWriteOperation =>
          log.info(period + ": " + pos + "/" + total)
          pos += 1
          if (y.getTime().before(start) || y.getTime().after(end)) {
            log.info("Ignoring date " + y.getTime())
            Seq()
          } else
            new ExpandOpToPeriod(readDataStore, writeDataStore, period).apply(y)
        case _:MigrationOperation => throw new InternalError("I expect a rw operation here")
      }
    }
    finer.toList
  }

  def expandInTime(ops:Seq[MigrationOperation], start:Date, end:Date, readDataStore:DataStore, writeDataStore:DataStore) = {
    val total = ops.size
    var pos = 0
    log.info("======== Calculating Yearly ========")
    val yearlies = ops flatMap {
      log.info("Yearly: " + pos + "/" + total)
      pos += 1
      y:MigrationOperation => new ExpandToRange(readDataStore, writeDataStore, Period.YEARLY, start, end).apply(y)
    }
    val monthlies = expandInPeriod(yearlies, Period.MONTHLY, start, end, readDataStore, writeDataStore)
    val dailies =  expandInPeriod(monthlies, Period.DAILY, start, end, readDataStore, writeDataStore)
    val deltas =  expandInPeriod(dailies, Period.HOURLY, start, end, readDataStore, writeDataStore)
    deltas.distinct.toList
  }

  def migrate(ops:Seq[MigrationOperation], start:Date, end:Date, readDataStore:DataStore, writeDataStore:DataStore, dryrun:Boolean) = {
    val deltas = expandInTime(ops, start, end, readDataStore, writeDataStore)
    log.info("======== Writing Metrics ========")
    deltas flatMap {
      d:MigrationOperation => new WriteMetric(writeDataStore, dryrun).apply(d)
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

  def preprocess(ops:Seq[MigrationOperation],start:Date, end:Date, readDataStore:DataStore, transform:MetricTransform) = {
    // Convert the recording into a list of Migration Read* operations
    log.info("======== Pass One ======== ")
    val passOne = ops flatMap { op:MigrationOperation => new ResolvedMetricToReadWrite(transform).apply(op) }
    passOne.foreach(log.trace(_))
    // transform all ReadChildrenOperations into ReadWrite by actually reading the parent entity/metric
    log.info("======== Pass Two ========")
    val passTwo = passOne flatMap { op:MigrationOperation => new ResolveChildrenToReadWrite(readDataStore, start, end).apply(op) }
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
       val rw = preprocess(recording, start, end, DataStoreFactory.get(), new MetricPartTransform(viewUid, destViewUid))
       migrate(rw, start, end, DataStoreFactory.get(), DataStoreFactory.get(), dryrun)
   }

   // copy 4x4/rows-accessed-download to 4x4/files-downloaded
   def esriToMondara(viewUid:ViewUid, start:Date, end:Date, dryrun:Boolean) = {
     val from = new ResolvedMetricOperation(viewUid, Fluff("rows-accessed-download"), RecordType.AGGREGATE)
     val to= new ResolvedMetricOperation(viewUid, Fluff("files-downloaded"), RecordType.AGGREGATE)
     val rw = ReadWriteOperation(from, to, None)
     migrate(Seq(rw), start, end, DataStoreFactory.get(), DataStoreFactory.get(), dryrun)
   }

   def viewsLoadedToViews(id:DomainId, start:Date, end:Date):Iterator[String] = {
     DataStoreFactory.get().find("views-loaded-" + id.toString(), start, end).asScala flatMap {
       m: Metrics =>
         m.keySet().asScala flatMap {
           name:String => Seq(name.replaceFirst("view-", ""))
         }
     }
   }

   def migrateDomain(src:DomainId, dest:DomainId, start:Date, end:Date, seeds:Array[String]) {
     val write = new PropertiesConfiguration()
     write.put("cassandra.servers", seeds.mkString(","))
     System.err.println("Using read configuration: " )
     System.err.println(Configuration.get())
     System.err.println("Using write configuration: " )
     System.err.println(write)
     Thread.sleep(10000)
     val readDataStore = DataStoreFactory.get()
     val writeDataStore = DataStoreFactory.get(write)

     val site = Seq(
       new FullEntityMetricOperation(src, RecordType.AGGREGATE),
       new FullEntityMetricOperation(MetricIdParts(src, Fluff("-applications")), RecordType.AGGREGATE),
       //new FullEntityMetricOperation(MetricIdParts(src, Fluff("-intern")), RecordType.AGGREGATE),
       new FullEntityMetricOperation(MetricIdParts(Fluff("views-loaded-"), src), RecordType.AGGREGATE),
       new FullEntityMetricOperation(MetricIdParts(Fluff("views-downloaded-"), src), RecordType.AGGREGATE)
     )
     val viewUIDs = viewsLoadedToViews(src, start, end)
     val viewMetrics = viewUIDs flatMap {
         uid:String => new MetricRecord().recordViews(ViewUid(uid), src)
     }
     val initial = site ++ viewMetrics
     //val initial = site
     log.info("Initial Set:")
     initial.foreach(log.info(_))
     val one = preprocess(initial, start, end, readDataStore, new MetricPartTransform(src, dest)).distinct
     log.info("Post Processed:")
     if (one.size > 0) {
       exit(1)
     }
     one.foreach(log.info(_))
     val writer:CSVWriter = new CSVWriter(new OutputStreamWriter(System.out, "UTF-8"), ',', '"');
     one.sliding(10) foreach {
       batch =>
         val deltas = expandInTime(batch, start, end, readDataStore, writeDataStore)
         // x-datacenter, but slow
         //deltas flatMap {
         //  d:MigrationOperation => new WriteMetric(writeDataStore, false).apply(d)
         //}

         // faster
         log.info("Initial Set Count: " + initial.size)
         log.info("Post Processed Count: " + one.size)
         log.info("Deltas Count: " + deltas.size)
         toCSV(deltas, writer)
         writer.flush()
     }
     writer.close()
   }

   def main(args: Array[String]) {
      val start = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
      start.set(2009, 1, 1, 0, 0, 0)
      val end = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
      end.set(2020, 1, 1, 0, 0, 0)

      val formatter = new SimpleDateFormat("yyyy-MM-dd");

      val cmd = if (args.length > 1) args(0) else "help"
      cmd.toString match {
        case "view" => syncViewMetrics(ViewUid(args(1)), ViewUid(args(2)), DomainId(args(3).toInt), start.getTime, end.getTime, !"real".equals(args(4)))
        case "esri2Mondara" => esriToMondara(ViewUid(args(1)), start.getTime, end.getTime, !"real".equals(args(2)))
        case "rsync-domain" => migrateDomain(DomainId(args(1).toInt), DomainId(args(2).toInt), formatter.parse(args(3)), formatter.parse(args(4)), args.drop(5))
        case "help" =>  {
          println("Migrator view [src view 4x4] [dst view 4x4] [domain id] [dryrun|real]")
          println("Migrator esri2Mondara view 4x4] [dryrun|real]")
          println("Migrator rsync-domain domainId destinationDomainId 2009-01-01 2014-01-1 write_seed1 [write_seed2...]")
          Seq()
        }
      }
   }
}


