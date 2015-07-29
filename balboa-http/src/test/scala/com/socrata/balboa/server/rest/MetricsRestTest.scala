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

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    val ds = DataStoreFactory.get()

    val cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"))
    cal.set(2015, Calendar.JANUARY, 10)

    ds.persist(entity, cal.getTimeInMillis, new Metrics(Map {
      "datasets-v2-published" -> new Metric(Metric.RecordType.ABSOLUTE, 3)
    }.asJava))
  }

  test("range") {
    get(s"metrics/$entity/range?start=2015-01-01&end=2015-02-01") {
      status should equal (200)
      JsonReader.fromString(body).dyn("datasets-v2-published").value.? should equal (Right(JNumber(3)))
    }
  }

}

