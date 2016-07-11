package com.socrata.metrics.components

import com.socrata.balboa.metrics.Metrics

case class BufferItem(entityId:String, metrics:Metrics, timestamp:Long)

trait BufferComponent {
  type Buffer <: BufferLike
  trait BufferLike {
    def add(bufferItem:BufferItem):Unit
    def size():Int
    def flush():Int
    def start():Unit
    def stop():Unit
  }

  // scalastyle:off method.name
  def Buffer():Buffer
}
