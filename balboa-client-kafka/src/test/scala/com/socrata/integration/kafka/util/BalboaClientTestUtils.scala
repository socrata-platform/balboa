package com.socrata.integration.kafka.util

import junit.framework.Assert._
import kafka.consumer.KafkaStream
import kafka.utils.TestUtils._

import scala.collection.Map

/**
 * Test Utilities for people who do not want to read through all the test utilities available in [[kafka.utils.TestUtils]].
 * These test utility functions are intended to be used with Balboa Related Testing Unit tests.
 */
object BalboaClientTestUtils {

  /**
   * Returns keys and messages tuples in a list.
   *
   * @param nMessagesPerThread Number of messages to expect on a thread
   * @param topicMessageStreams Mapping of topic and KafkaStreams for that topic.
   * @tparam K Kafka Key Type
   * @tparam M Kafka Message Type
   * @return List of Key Message pairs returned
   */
  def getKeysAndMessages[K,M](nMessagesPerThread: Int,
                              topicMessageStreams: Map[String, List[KafkaStream[K, M]]]): List[(K,M)] = {
    var keysAndMessages: List[(K,M)] = Nil
    for ((topic, messageStreams) <- topicMessageStreams) {
      for (messageStream <- messageStreams) {
        val iterator = messageStream.iterator
        for (i <- 0 until nMessagesPerThread) {
          assertTrue(iterator.hasNext)
          val mam = iterator.next()
          val tuple = (mam.key(), mam.message())
          keysAndMessages ::= tuple
          debug("Received Key and Message: " + tuple)
        }
      }
    }
    keysAndMessages.reverse
  }

}
