package com.socrata.balboa.common.util

import java.util.{Calendar, TimeZone}

import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.metrics.impl.JsonMessage
import com.socrata.balboa.metrics.{Metric, Metrics}

/**
 * Balboa Testing Objects.
 */
object MetricsTestStuff {

  trait TestEntityIDs {
    // For Lack of a better entity ID name.
    val balboa = "balboa"

    val allEntityIDs = Seq(balboa)
  }

  /**
   * Metrics
   */
  trait TestMetrics {
    val cats = ("cats", metric(1))
    val giraffes = ("giraffes", metric(2))
    val dogs = ("dogs", metric(3))
    val penguins = ("penguins", metric(1000000))
    val monkeys = ("monkeys", metric(5))
    val rhinos = ("rhinos", metric(5))
    val abs_alive_cats = ("alive_cats", metric(100, RecordType.ABSOLUTE))
    val abs_alive_penguins = ("alive_penguins", metric(100, RecordType.ABSOLUTE))
    val emptyMetrics = new Metrics()
    val oneElemMetrics = metrics(cats, abs_alive_cats)
    val manyElemMetrics = metrics(cats, giraffes, dogs, penguins, monkeys, rhinos, abs_alive_penguins)
    val allMetrics = Seq(emptyMetrics, oneElemMetrics, manyElemMetrics)
  }

  /**
   * TimeStamps for Balboa testing.
   */
  trait TestTimeStamps {
    val cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"))

    // Times Within the same hour
    cal.set(2015, 1, 1, 12, 0, 0)
    val t20150101_1200_0 = cal.getTime
    cal.set(2015, 1, 1, 12, 1, 0)
    val t20150101_1201_0 = cal.getTime
    cal.set(2015, 1, 1, 12, 2, 0)
    val t20150101_1202_0 = cal.getTime
    cal.set(2015, 1, 1, 12, 3, 0)
    val t20150101_1203_0 = cal.getTime
    cal.set(2015, 1, 1, 12, 4, 0)
    val t20150101_1204_0 = cal.getTime
    cal.set(2015, 1, 1, 12, 5, 0)
    val t20150101_1205_0 = cal.getTime

    val sameHour = Seq(
      t20150101_1200_0,
      t20150101_1201_0,
      t20150101_1202_0,
      t20150101_1203_0,
      t20150101_1204_0,
      t20150101_1205_0)

    // Times Within the same day
    cal.set(2015, 1, 1, 13, 0, 0)
    val t20150101_1300_0 = cal.getTime
    cal.set(2015, 1, 1, 14, 0, 0)
    val t20150101_1400_0 = cal.getTime
    cal.set(2015, 1, 1, 15, 0, 0)
    val t20150101_1500_0 = cal.getTime
    cal.set(2015, 1, 1, 16, 0, 0)
    val t20150101_1600_0 = cal.getTime

    val sameDay = sameHour ++ Seq(t20150101_1300_0,
      t20150101_1400_0,
      t20150101_1500_0,
      t20150101_1600_0)

    // Times within the same month
    cal.set(2015, 1, 2, 12, 0, 0)
    val t20150102_1200_0 = cal.getTime
    cal.set(2015, 1, 3, 12, 0, 0)
    val t20150103_1200_0 = cal.getTime
    cal.set(2015, 1, 4, 12, 0, 0)
    val t20150104_1200_0 = cal.getTime
    cal.set(2015, 1, 5, 12, 0, 0)
    val t20150105_1200_0 = cal.getTime

    val sameMonth = sameDay ++ Seq(
      t20150102_1200_0,
      t20150103_1200_0,
      t20150104_1200_0,
      t20150105_1200_0
    )

    // Times within the same year
    cal.set(2015, 2, 1, 12, 0, 0)
    val t20150201_1200_0 = cal.getTime
    cal.set(2015, 3, 1, 12, 0, 0)
    val t20150301_1200_0 = cal.getTime
    cal.set(2015, 4, 1, 12, 0, 0)
    val t20150401_1200_0 = cal.getTime
    cal.set(2015, 5, 1, 12, 0, 0)
    val t20150501_1200_0 = cal.getTime

    val sameYear = sameMonth ++ Seq(
      t20150201_1200_0,
      t20150301_1200_0,
      t20150401_1200_0,
      t20150501_1200_0
    )

    val allTimeStamps = sameYear
  }

  /**
   * Precomposed messages.
   */
  trait TestMessages extends TestMetrics {
    val emptyMessage = message("empty", emptyMetrics, 1)
    val oneElemMessage = message("empty", oneElemMetrics, 1)
    val manyElemMessage = message("empty", manyElemMetrics, 1)
  }

  def metric(value: Number, t: RecordType = RecordType.AGGREGATE): Metric = {
    val m = new Metric()
    m.setType(t)
    m.setValue(value)
    m
  }

  def metrics(metrics: (String, Metric)*): Metrics = {
    val ms = new Metrics()
    metrics.foreach(m => ms.put(m._1, m._2))
    ms
  }

  def message(entityId: String, metrics: Metrics, time: Long = 1): JsonMessage = {
    val m = new JsonMessage()
    m.setEntityId(entityId)
    m.setTimestamp(time)
    m.setMetrics(metrics)
    m
  }

}
