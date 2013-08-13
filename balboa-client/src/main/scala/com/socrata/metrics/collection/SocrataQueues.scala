package com.socrata.metrics.collection

import java.util.concurrent.{TimeUnit, LinkedBlockingQueue}
import scala.collection.mutable.Queue

trait SocrataAbstractQueue[Q] {
  def add(a:Q):Unit
  def take():Option[Q]
  def isEmpty:Boolean
  def hasValues = !isEmpty
  def size:Int
}

//TODO: make configurable and cap size.
class SocrataLinkedBlockingQueue[Q](queue:LinkedBlockingQueue[Q]) extends SocrataAbstractQueue[Q] {
  def add(a:Q) { queue.add(a)}
  def take() = Option(queue.poll(1,TimeUnit.SECONDS))
  def isEmpty = queue.isEmpty
  def size = queue.size

}

class SocrataQueue[Q](queue:Queue[Q]) extends SocrataAbstractQueue[Q] {
  def add(a:Q) {queue.enqueue(a)}
  def take() = Some(queue.dequeue())
  def isEmpty = queue.isEmpty
  def size = queue.size
}
