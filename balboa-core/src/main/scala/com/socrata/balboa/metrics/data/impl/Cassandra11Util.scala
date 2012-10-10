package com.socrata.balboa.metrics.data.impl

import com.netflix.astyanax.{Keyspace, AstyanaxContext}
import com.netflix.astyanax.connectionpool.impl.{CountingConnectionPoolMonitor, ConnectionPoolConfigurationImpl}
import com.socrata.balboa.metrics.data.{DateRange, Period}
import com.socrata.balboa.metrics.config.Configuration
import com.socrata.balboa.metrics.Metric.RecordType
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.StringSerializer
import com.netflix.astyanax.AstyanaxContext.Builder
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl
import com.netflix.astyanax.connectionpool.NodeDiscoveryType
import com.netflix.astyanax.thrift.ThriftFamilyFactory
import java.{util => ju}
import com.socrata.balboa.metrics.Timeslice
import scala.{collection => sc}

/**
 * Holds Connection Pool and Common ColumnFamily definitions
 */
object Cassandra11Util {
  val periods = Configuration.get().getSupportedPeriods()
  val leastGranular:Period = Period.leastGranular(periods)
  val context:AstyanaxContext[Keyspace] = initializeContext()

  def getContext = context

  def sliceIterator(queryImpl:Cassandra11Query, entityId:String, period:Period, query:List[ju.Date]):Iterator[Timeslice] = {
    query.iterator.map { date =>
          val range = DateRange.create(period, date)
          new Timeslice(range.start.getTime, range.end.getTime, queryImpl.fetch(entityId, period, date))
    }.filter(_ != null)
  }

  def metricsIterator(queryImpl:Cassandra11Query, entityId:String, query:sc.Map[Period, List[ju.Date]]) = {
    {
      for {
        (period, dates) <- query.iterator
        date <- dates.iterator
      } yield queryImpl.fetch(entityId, period, date)
    }.filter(_ != null)
  }

  def getColumnFamily(period:Period, recordType:RecordType):ColumnFamily[String, String] = {
    new ColumnFamily[String, String](period.toString + "_" + recordType.toString, StringSerializer.get(), StringSerializer.get())
  }

  def createEntityKey(entityId:String, timestamp:Long) = entityId + "-" + timestamp

  def initializeContext():AstyanaxContext[Keyspace] = {
    val conf = Configuration.get()
    val seeds = conf.getProperty("cassandra.servers")
    val keyspace = conf.getProperty("cassandra.keyspace")
    val sotimeout = conf.getProperty("cassandra.sotimeout").toInt
    val connections = conf.getProperty("cassandra.maxpoolsize").toInt
    val cxt:AstyanaxContext[Keyspace] = new Builder()
      .forKeyspace(keyspace)
      .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
        .setDiscoveryType (NodeDiscoveryType.RING_DESCRIBE)
      )
      .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("BalboaPool")
        .setConnectTimeout(sotimeout)
        .setTimeoutWindow(sotimeout)
        .setMaxConnsPerHost(connections)
        .setSeeds(seeds))
      .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
      .buildKeyspace(ThriftFamilyFactory.getInstance())
    cxt.start()
    cxt
  }
}
