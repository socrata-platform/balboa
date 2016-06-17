package com.socrata.balboa.metrics.data.impl

import java.{util => ju}

import com.netflix.astyanax.AstyanaxContext.Builder
import com.netflix.astyanax.connectionpool.NodeDiscoveryType
import com.netflix.astyanax.connectionpool.impl.{ConnectionPoolConfigurationImpl, CountingConnectionPoolMonitor}
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.StringSerializer
import com.netflix.astyanax.thrift.ThriftFamilyFactory
import com.netflix.astyanax.{AstyanaxContext, Keyspace}
import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.metrics.{Metrics, Timeslice}
import com.socrata.balboa.metrics.config.Configuration
import com.socrata.balboa.metrics.data.{DateRange, Period}
import com.typesafe.scalalogging.slf4j.StrictLogging

import scala.{collection => sc}

/**
 * Holds Connection Pool and Common ColumnFamily definitions
 */
object Cassandra11Util extends StrictLogging {
  val periods = Configuration.get().getSupportedPeriods
  val leastGranular:Period = Period.leastGranular(periods)
  val mostGranular:Period = Period.mostGranular(periods)

  /*
   * Roll up a timeslice into another, more granular, unsupported Period
   *
   * The resulting set should lie on the requested Period boundaries but it
   * may have more items than if the requested Period were supported directly
   *
   */
  def rollupSliceIterator(period: Period, raw: Iterator[Timeslice]): Iterator[Timeslice] = {

    // can't do tail rec mod cons
    // @tailrec
    def loop(input: Stream[Timeslice], acc: Timeslice, range: DateRange): Stream[Timeslice] = {
      if(input.isEmpty) {
        Stream(acc)
      } else {
        val c = input.head
        if(c.getEnd <= range.end.getTime) {
          acc.addTimeslice(c)
          loop(input.tail, acc, range)
        } else {
          val newRange = DateRange.create(period, new ju.Date(c.getStart))
          c.setStart(newRange.start.getTime)
          c.setEnd(newRange.end.getTime)
          acc #:: loop(input.tail, c, newRange)
        }
      }
    }

    if(raw.hasNext) {
      val firstTimeslice = raw.next()
      val firstRange = DateRange.create(period,new ju.Date(firstTimeslice.getStart))
      firstTimeslice.setStart(firstRange.start.getTime)
      firstTimeslice.setEnd(firstRange.end.getTime)
      loop(raw.toStream, firstTimeslice, firstRange).iterator
    } else {
      Iterator.empty
    }
  }
  def sliceIterator(queryImpl:Cassandra11Query, entityId:String, period:Period, query:List[ju.Date]):Iterator[Timeslice] = {
    query.iterator.map { date =>
          val range = DateRange.create(period, date)
          new Timeslice(range.start.getTime, range.end.getTime, queryImpl.fetch(entityId, period, date))
    }.filter(_ != null)
  }

  def metricsIterator(queryImpl: Cassandra11Query,
                      entityId: String,
                      query: sc.Seq[(ju.Date, Period)]): Iterator[Metrics] = {
    {
      for {
        (date, period) <- query.iterator
      } yield queryImpl.fetch(entityId, period, date)
    }.filter(_ != null)
  }

  def getColumnFamily(period:Period, recordType:RecordType):ColumnFamily[String, String] = {
    new ColumnFamily[String, String](period.toString + "_" + recordType.toString, StringSerializer.get(), StringSerializer.get())
  }

  def createEntityKey(entityId:String, timestamp:Long): String = entityId + "-" + timestamp

  def initializeContext():AstyanaxContext[Keyspace] = {
    initializeContext(Configuration.get())
  }

  def initializeContext(conf:Configuration):AstyanaxContext[Keyspace] = {

    val seeds = conf.getProperty("cassandra.servers")
    val keyspace = conf.getProperty("cassandra.keyspace")
    val datacenter = Option(conf.getProperty("cassandra.datacenter"))
    val sotimeout = conf.getProperty("cassandra.sotimeout").toInt
    val connections = conf.getProperty("cassandra.maxpoolsize").toInt

    logger.info("Connecting to Cassandra servers '{}'", seeds)
    logger.info(
      datacenter.fold
        ("Defaulting Cassandra client configuration to use all available datacenters")
        (datacenter_name => s"Configuring Cassandra client for datacenter-local in $datacenter_name")
    )
    logger.info("Using maximum size of '{}' for Cassandra connection pool.", connections.toString)
    logger.info("Setting Cassandra socket timeout to '{}'", sotimeout.toString)
    logger.info("Using keyspace '{}'", keyspace)

    val connectionPoolConfiguration = new ConnectionPoolConfigurationImpl("BalboaPool")
      .setConnectTimeout(sotimeout)
      .setTimeoutWindow(sotimeout)
      .setMaxConnsPerHost(connections)
      .setSeeds(seeds)

    // Set local DC as side-effect if specified in configuration.
    // If the local datacenter is specified it will limit the driver
    // to only make connections to the Cassandra nodes in the datacenter
    // and prevent this service from unintentionally reaching across a
    // VPN to connect to a Cassandra node.
    datacenter.foreach(connectionPoolConfiguration.setLocalDatacenter)

    val cxt:AstyanaxContext[Keyspace] = new Builder()
      .forKeyspace(keyspace)
      .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
        .setDiscoveryType (NodeDiscoveryType.RING_DESCRIBE)
      )
      .withConnectionPoolConfiguration(connectionPoolConfiguration)
      .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
      .buildKeyspace(ThriftFamilyFactory.getInstance())
    cxt.start()
    cxt
  }
}
