package com.socrata.metrics.util

import com.socrata.balboa.common.Metrics
import com.socrata.metrics.MetricQueue

import scala.collection.SetLike

/**
 * Buffer that does not allow multiple Metric Entries that have the same ids.  This buffer is a very light wrapper
 * around a set that ensures the duplicate entries are merged before they are added together.
 *
 * Reference: [[MetricEntry]]
 */
class MetricEntryBuffer(underlying: Set[MetricEntry]) extends Set[MetricEntry] with SetLike[MetricEntry, MetricEntryBuffer] {

  // Composition pattern but makes it so this class is mutable. TODO Find a better pattern for this.
  var buffer = underlying
  
  /** Reference: [[Set.empty]] */
  override def empty: MetricEntryBuffer = new MetricEntryBuffer(Set.empty)
  
  /**
   * Checks to see if there is an equal Metric Entry before insertion.  If there is an equal entry it merges the the
   * existing entry with the new element.  If there doesn't exist a duplicate version then the behaviour mimics that of
   * a traditional Set.
   *
   * Reference: [[SetLike.+()]]
   */
  override def +(newElem: MetricEntry): MetricEntryBuffer = newElem match {
    case e: MetricEntry =>
      underlying.find(m => m.equals(e)) match {
        case Some(inBuffer) =>
          buffer = buffer + inBuffer.merge(newElem)
        case None =>
          buffer = buffer + newElem
      }
      this
  }

  /** Reference: [[SetLike.contains()]] */
  override def contains(elem: MetricEntry): Boolean = underlying.contains(elem)

  /** Reference: [[SetLike.-()]] */
  override def -(elem: MetricEntry): MetricEntryBuffer = {
    buffer = buffer - elem
    this
  }

  /** Reference: [[SetLike.iterator]] */
  override def iterator: Iterator[MetricEntry] = buffer.iterator
}

/**
 * A single Metric entry
 */
sealed trait MetricEntry {

  /**
   * Merges this entry with the other if they are equal.
   */
  def merge(other: MetricEntry): MetricEntry

}
case class Cons(entityId: String, timestamp: Long, metrics: Metrics) extends MetricEntry {

  /** ID Based off of aggregation */
  val id: String = entityId + ":" + (timestamp - (timestamp % MetricQueue.AGGREGATE_GRANULARITY))

  /**
   * Merges this entry with the other if they are equal.  If this was not able to merge just returns this.
   */
  override def merge(other: MetricEntry): MetricEntry = other match {
    case Cons(e, t, m) if this.equals(other) => this.metrics.merge(m)
    this
  }

  override def equals(obj: scala.Any): Boolean = obj match {
    case that: Cons => (that canEqual this) && super.equals(that) && this.id.equals(that.id)
    case _ => false
  }

  override def hashCode(): Int = 41 * id.hashCode()

  override def canEqual(that: Any): Boolean = that.isInstanceOf[Cons]

}

object MetricEntry {

  /**
   * Creates an instance of a Metric Entry based off the entity id.
   *
   * @param entityId The entity id that this set of metrics belongs to.
   * @param timestamp The time that the metrics occured
   * @param metrics The collection of metrics to be recordered
   * @return Some(entry) for successful instance creation, None otherwise
   */
  def cons(entityId: String, timestamp: Long, metrics: Metrics): Option[MetricEntry] =
    (entityId, timestamp, metrics) match {
      // Just ensure no nulls are being passed through
      case (e: String, t: Long, m: Metrics) => Some(Cons(e, t, m))
      case _ => None
  }

}