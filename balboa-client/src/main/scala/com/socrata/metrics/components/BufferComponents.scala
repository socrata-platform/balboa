package com.socrata.metrics.components

import com.socrata.balboa.common.Metrics

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

  def Buffer():Buffer
}
