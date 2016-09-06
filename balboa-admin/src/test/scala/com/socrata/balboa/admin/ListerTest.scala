package com.socrata.balboa.admin

import com.socrata.balboa.admin.tools.Lister
import com.socrata.balboa.metrics.data.{DataStore, DataStoreFactory}
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.mockito.Matchers.{eq => eqTo}

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

class ListerTest extends FlatSpec with Matchers with MockitoSugar  {
  val dsf = mock[DataStoreFactory]
  val datastore = mock[DataStore]
  val entities = List(1, 2, 4, 3, 2, 5).map(_.toString)
  val filter = "filter"
  when(dsf.get).thenReturn(datastore)
  when(datastore.entities()).thenReturn(entities.iterator)
  when(datastore.entities(eqTo[String](filter))).thenReturn(List("2", "2").iterator)
  val lister = new Lister(dsf)

  "list() with no filters" should "print all the entities directly, removing duplicates" in {
    val printed = new ListBuffer[String]
    lister.list(Nil.iterator, entity => printed += entity)
    printed.toList should be (entities.distinct)
  }

  "list() with filters" should "print all entities passing any of the filters, removing duplicates" in {
    val printed = new ListBuffer[String]
    lister.list(List(filter).iterator, entity => printed += entity)
    printed.toList should be (List("2"))
  }

  "list() from java with no filters" should "not throw errors" in {
    lister.listJava(List.empty[String].iterator.asJava)
  }
}
