package com.socrata.balboa.metrics.data.impl

// scalastyle: off file.size.limit

import java.net.InetSocketAddress
import java.{util => ju}

import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, DefaultRetryPolicy, LoggingRetryPolicy}
import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.metrics.{Metrics, Timeslice}
import com.socrata.balboa.metrics.config.Configuration
import com.socrata.balboa.metrics.data.{DateRange, Period}
import com.typesafe.scalalogging.slf4j.StrictLogging
import com.datastax.driver.core._

import scala.util.Try
import scala.{collection => sc}

/**
 * Holds Connection Pool and Common ColumnFamily definitions
 */
object CassandraUtil extends StrictLogging {
  val periods = Configuration.get().getSupportedPeriods
  val leastGranular:Period = Period.leastGranular(periods)
  val mostGranular:Period = Period.mostGranular(periods)

  case class DatastaxContext(cluster: Cluster, _keyspace: String) {
    def keyspace: String = {
      // Mixed case names are automatically lower cased somewhere along the
      // way to Cassandra. So keyspaces that have mixed case names must be
      // quoted to preserve case.
      if (_keyspace != _keyspace.toLowerCase) {
        '"' + _keyspace + '"'
      } else {
        _keyspace
      }
    }

    def newSession: Session = cluster.connect(keyspace)

    def execute(stmt: Statement): ju.List[Row] = {
      val session = this.newSession
      val result = Try(session.execute(stmt).all())
      session.close()
      result.get
    }

    def executeUpdate(stmt: Statement): Unit = {
      val session = this.newSession
      try {
        session.execute(stmt)
      } finally {
        session.close()
      }
    }
  }

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
  def sliceIterator(queryImpl:CassandraQuery,
                    entityId:String,
                    period:Period,
                    query:List[ju.Date]):Iterator[Timeslice] = {
    query.iterator.map { date =>
          val range = DateRange.create(period, date)
          new Timeslice(range.start.getTime, range.end.getTime, queryImpl.fetch(entityId, period, date))
    }.filter(Option(_).isDefined)
  }

  def metricsIterator(queryImpl: CassandraQuery,
                      entityId: String,
                      query: sc.Seq[(ju.Date, Period)]): Iterator[Metrics] = {
    {
      for {
        (date, period) <- query.iterator
      } yield queryImpl.fetch(entityId, period, date)
    }.filter(Option(_).isDefined)
  }

  def getColumnFamily(period:Period, recordType:RecordType):String = {
    period.toString + "_" + recordType.toString
  }

  def createEntityKey(entityId:String, timestamp:Long): String = entityId + "-" + timestamp

  def initializeContext():DatastaxContext = {
    initializeContext(Configuration.get())
  }

  // scalastyle: off
  def initializeContext(conf:Configuration): DatastaxContext = {

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


    // Set local DC as side-effect if specified in configuration.
    // If the local datacenter is specified it will limit the driver
    // to only make connections to the Cassandra nodes in the datacenter
    // and prevent this service from unintentionally reaching across a
    // VPN to connect to a Cassandra node.

    val dcPolicy = DCAwareRoundRobinPolicy.builder()
    datacenter.foreach(dc => dcPolicy.withLocalDc(dc))

    val poolingOptions = new PoolingOptions()
      .setMaxConnectionsPerHost(HostDistance.LOCAL, connections)
      .setMaxConnectionsPerHost(HostDistance.REMOTE, connections)

    val socketOptions = new SocketOptions()
      .setConnectTimeoutMillis(sotimeout)

    val seedInetAddrs = seeds.split(",").map(addrStr => {
      val addrAndPort = addrStr.trim().split(":")
      if (addrAndPort.length != 2) {
        throw new IllegalArgumentException("Address and port must be separated by a ':' in: " + addrStr)
      }
      new InetSocketAddress(addrAndPort(0), addrAndPort(1).toInt)
    })

    logger info s"Connecting to Cassandra on seed addresses: ${ seedInetAddrs.mkString(", ") }"

    val cluster = Cluster.builder()
      .addContactPointsWithPorts(seedInetAddrs:_*)
      .withPoolingOptions(poolingOptions)
      .withSocketOptions(socketOptions)
      .withLoadBalancingPolicy(dcPolicy.build())
      .withRetryPolicy(new LoggingRetryPolicy(DefaultRetryPolicy.INSTANCE))
      .build()

    DatastaxContext(cluster, keyspace)
  }
}
