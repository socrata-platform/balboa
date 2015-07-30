package com.socrata.balboa.producer.config

import com.socrata.balboa.common.config.{Configuration, Keys}
import org.scalatest.WordSpec

/**
 * Spec Tests for Dispatcher.
 */
class DispatcherConfigSpec extends WordSpec {

  "A DispatcherConfig" should {

    "return an empty list when configuration has empty comma separated list" in {
      Configuration.get().put(Keys.DISPATCHER_CLIENT_TYPES, "")
      assert(DispatcherConfig.clientTypes.length == 0, "Client types should be empty.")
    }

    "return a list of valid Client Types" in {
      Configuration.get().put(Keys.DISPATCHER_CLIENT_TYPES, ClientType.values.map(_.toString).mkString(","))
      assert(DispatcherConfig.clientTypes.length == ClientType.values.size, "All of the client types should be " +
        "included")
    }

    "Filter out and log a warning when a client type is not recognized" in {
      Configuration.get().put(Keys.DISPATCHER_CLIENT_TYPES, ClientType.values.map(_.toString).mkString(",") +
        ",fake_client")
      assert(DispatcherConfig.clientTypes.length == ClientType.values.size, "All of the client types should be " +
        "included")
    }
  }

}
