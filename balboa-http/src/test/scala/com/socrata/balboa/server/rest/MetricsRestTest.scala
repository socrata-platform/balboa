package com.socrata.balboa.server.rest

import java.util.{Calendar, GregorianCalendar, TimeZone}

import com.rojoma.json.v3.ast.JNumber
import com.rojoma.json.v3.io.JsonReader
import com.socrata.balboa.metrics.data.DataStoreFactory
import com.socrata.balboa.metrics.{Metric, Metrics}
import com.socrata.balboa.server.Main
import org.scalatra.test.scalatest._

import scala.collection.JavaConverters._

class MetricsRestTest extends ScalatraFunSuite {

  addServlet(Main.Servlet, "/*")

  val entity = "1"
  val absoluteMetric = "datasets"
  val aggregateMetric = "datasets-created"

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    System.clearProperty("socrata.env")
    val ds = DataStoreFactory.get()

    val cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"))
    cal.set(2015, Calendar.JANUARY, 10)

    ds.persist(entity, cal.getTimeInMillis, new Metrics(Map(
      absoluteMetric -> new Metric(Metric.RecordType.ABSOLUTE, 3),
      aggregateMetric -> new Metric(Metric.RecordType.AGGREGATE, 2)
    ).asJava))

    cal.set(2015, Calendar.FEBRUARY, 10)

    ds.persist(entity, cal.getTimeInMillis, new Metrics(Map(
      absoluteMetric -> new Metric(Metric.RecordType.ABSOLUTE, 1),
      aggregateMetric -> new Metric(Metric.RecordType.AGGREGATE, 4)
    ).asJava))
  }

  test("range") {
    get(s"metrics/$entity/range?start=2015-01-01&end=2015-02-01") {
      status should equal (200)
      val json = JsonReader.fromString(body)
      json.dyn(absoluteMetric).value.? should equal (Right(JNumber(3)))
      json.dyn(aggregateMetric).value.? should equal (Right(JNumber(2)))
    }

    get(s"metrics/$entity/range?start=2015-01-01&end=2015-03-01") {
      status should equal (200)
      val json = JsonReader.fromString(body)
      json.dyn(absoluteMetric).value.? should equal (Right(JNumber(1)))
      json.dyn(aggregateMetric).value.? should equal (Right(JNumber(6)))
    }
  }

}

