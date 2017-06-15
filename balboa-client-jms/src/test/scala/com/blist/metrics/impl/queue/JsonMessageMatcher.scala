package com.blist.metrics.impl.queue

import com.socrata.balboa.metrics.impl.JsonMessage
import org.mockito.ArgumentMatcher

class JsonMessageMatcher(expected: JsonMessage) extends ArgumentMatcher[String] {
  def matches(argument: String): Boolean = {
    val parsedMessage = JsonMessage(argument)
    parsedMessage.getMetrics.equals(expected.getMetrics) && JsonMessage(argument) == expected
  }

  override def toString: String = s"""\"${expected}\"""""
}
