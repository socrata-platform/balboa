package com.socrata.balboa.metrics.data

/** FlatMap for Java */
class CompoundIterator[T](underlying: Iterator[Iterator[T]]) extends Iterator[T] {
  val result = underlying.flatten

  override def hasNext: Boolean = result.hasNext
  override def next(): T = result.next()
}
