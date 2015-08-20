package com.socrata.metrics.collection

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

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

