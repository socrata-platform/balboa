package com.socrata.balboa.jms

import java.net.URI
import java.util
import java.util.{Date, Optional}

import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.metrics.data.{DataStore, Period}
import com.socrata.balboa.metrics.{Metrics, Timeslice}
import org.hawkular.inventory.paths.RelativePath.MetricBuilder
import org.hawkular.metrics.model.param.Tags
import org.hawkular.metrics.model.{MetricId, MetricType, DataPoint, Metric}
import org.hawkular.inventory.paths.{SegmentType, CanonicalPath}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import org.hawkular.client.core.{HawkularClient, ClientInfo}

/**
 * Created by andrewgall on 11/3/16.
 */
class HawkularMetricStore(hawkularServerAddress: URI, username: String, password: String, token: String) extends DataStore {
  val clientInfo = new ClientInfo(
    hawkularServerAddress,
    Optional.of(username),
    Optional.of(password),
    Map[String,Object]("Hawkular-Admin-Token" -> token).asJava)
  val client = new HawkularClient(clientInfo)
  val metricClient = client.metrics()

  val log = LoggerFactory.getLogger(classOf[HawkularMetricStore])

  /**
   * Throws an exception if it cannot perform a query to Cassandra
   */
  override def checkHealth(): Unit = {
    client.metrics().counter.getCounters(new Tags(new util.HashMap[String,String]()))
    Unit
  }

  /**
   * Return a list of metrics for a period of timeslices over an arbitrary
   * date range, chronologically ascending.
   *
   * e.g. Give me all of the metrics for some entity broken down by hours in
   * the range 2010-01-01 -> 2010-01-31.
   */
  override def slices(entityId: String, period: Period, start: Date, end: Date): Iterator[Timeslice] = ???

  /**
   * Retrieve an iterator that contains all the entity ids that the pattern
   * string matches. You probably don't want to use this, because it's
   * terribly slow and possibly dangerous.
   */
  override def entities(pattern: String): Iterator[String] = ???

  /**
   * Retrieve an iterator that encompasses all entity ids for which there are
   * metrics being tracked. Generally speaking, this query is likely to be
   * very expensive and you probably don't want to use it unless you know
   * what you're doing.
   */
  override def entities(): Iterator[String] = ???


  val entityContainsDomainId = """([a-z\-]+[a-z]+)-([0-9]+)""".r
  val metricContainsUid = """[a-z\-]+[a-z]+-([0-9a-z]{4}-[0-9a-z]{4})""".r
  /**
   * Save a set of metrics. The datastore is responsible for making sure the
   * persist applies correctly to all supported tiers.
   */
  override def persist(entityId: String, timestamp: Long, metrics: Metrics): Unit = {
    entityId match {
      case entityContainsDomainId() => {
        entityContainsDomainId.findAllIn(entityId).matchData.foreach((m) => {
          val metricName = m.group(1)
          val domainId = m.group(2)

          val (counters, gauges) = metrics.asScala.foldLeft((Seq[Metric[java.lang.Long]](), Seq[Metric[java.lang.Double]]()))((accum, metricTuple) => {
            metricContainsUid.findAllIn(metricName).matchData.foldLeft(accum)((accum, m) => {
              val (counters, gauges) = accum
              val (metricName, rawMetric) = metricTuple
              val uid = m.group(1)
              val canonicalPath = CanonicalPath.of().tenant(domainId).get().extend(SegmentType.r, uid)

              rawMetric.getType match {
                case RecordType.ABSOLUTE => {
                  val dataPoints: util.List[DataPoint[java.lang.Long]] = new util.ArrayList[DataPoint[java.lang.Long]]()
                  dataPoints.add(new DataPoint(timestamp, rawMetric.getValue.longValue()))

                  val metricId = new MetricId(domainId,MetricType.COUNTER,metricName)
                  ((counters ++ Seq(new Metric[java.lang.Long](metricId, dataPoints))), gauges)
                }
                case RecordType.AGGREGATE => {
                  val dataPoints: util.List[DataPoint[java.lang.Double]] = new util.ArrayList[DataPoint[java.lang.Double]]()
                  dataPoints.add(new DataPoint(timestamp, rawMetric.getValue.doubleValue()))

                  val metricId = new MetricId(domainId,MetricType.GAUGE,metricName)
                  (counters, (gauges ++ Seq(new Metric[java.lang.Double](metricId, dataPoints))))
                }
              }
            })
          })

          metricClient.counter().addCounterData(counters.asJava)
          metricClient.gauge().addGaugeData(gauges.asJava)
        })
      }
      case _ => {
        log.info("Ignoring a metric because it does not have a domain in it")
      }
    }
  }

  /**
   * Given a date and given a summary range period, create the appropriate range
   * for the date (explained below) and perform a query that returns all the
   * summaries for that time period.
   *
   * The range is created by taking the period and finding the boundaries for
   * that period that the date belongs to. For example is the period is "month"
   * and the date is 2010-01-04, the range that will be queried is
   * 2010-01-01 -> 2010-01-31. For more details
   *
   * @see DateRange
   */
  override def find(entityId: String, period: Period, date: Date): Iterator[Metrics] = ???

  /**
   * Find all the summaries of a particular tier between start and end, ordered
   * by date. This is not necessarily the most optimal way to query for an
   * arbitrary range and should only be used when you need to query a specific
   * tier for some reason.
   */
  override def find(entityId: String, period: Period, start: Date, end: Date): Iterator[Metrics] = ???

  /**
   * Find the total summaries between two particular dates, ordered by date.
   * The query optimizer should plan the query so that start and end align
   * along a date date boundary of your most granular type.
   *
   * @see com.socrata.balboa.metrics.data.Period
   */
  override def find(entityId: String, start: Date, end: Date): Iterator[Metrics] = ???

  override def ensureStarted(): Unit = ???

  override def onStart(): Unit = ???

  override def heartbeat(): Unit = ???

  override def onStop(): Unit = ???
}
