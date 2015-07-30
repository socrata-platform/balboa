package com.socrata.balboa.common

import org.scalatest.WordSpec

class AddressAndPortSpec extends WordSpec {

  "An AddressAndPort" should {

    "return an empty list with an empty String" in {
      var result = AddressAndPort.parse("")
      assert(result.size == 0)
      result = AddressAndPort.parse(null)
      assert(result.size == 0)
    }

    "return an empty list with mislabeled host and ports" in {
      // Forgotten Port
      var result = AddressAndPort.parse("0.0.0.0")
      assert(result.size == 0)
      // Forgotten ":"
      result = AddressAndPort.parse("0.0.0.08000")
      assert(result.size == 0)
    }

    "return a single item for a list of actually one element" in {
      val result = AddressAndPort.parse("127.0.0.1:8000")
      assert(result.size == 1)
    }

    "return a item when given a DNS name" in {
      val result = AddressAndPort.parse("www.google.com:8000")
      assert(result.size == 1)
      assert(result(0).address.getHostName == "www.google.com", "Incorrect name for " + result(0).address.getHostName)
      assert(result(0).port == 8000, "Incorrect Port")
    }

    "return a list of brokers given a correctly formatted comma separated list" in {
      val result = AddressAndPort.parse("0.0.0.0:4000,127.0.0.1:3333,www.google.com:8080")
      assert(result.size == 3)
    }
  }

}
