package com.socrata.balboa.jms

import com.typesafe.config.Config
import org.scalatest.{MustMatchers, WordSpec}
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import com.socrata.balboa.jms.BalboaJmsTest.configFromTimeouts

class BalboaJmsTest extends WordSpec with MustMatchers {

  "BalboaJms::parseServers" should {
    "parse a single argument as one or more servers" in {
      {
      val args = Array(
        "failover:(uri1,uri2)?transportOptions&nestedURIOptions")
      val servers = BalboaJms.parseServers(args, configFromTimeouts())
      servers.size must be (1)
      }

      {
      val args = Array(
        "failover:(uri1,uri2)?transportOptions&nestedURIOptions," +
        "failover:(uri3,uri4)?transportOptions&nestedURIOptions")
      val servers = BalboaJms.parseServers(args, configFromTimeouts())
      servers.size must be (2)
      }
    }

    "produce servers from n arguments" in {
      {
      val args = Array(
        "failover:(uri1,uri2)?transportOptions&nestedURIOptions",
        "failover:(uri3,uri4)?transportOptions&nestedURIOptions")
      val servers = BalboaJms.parseServers(args, configFromTimeouts())
      servers.size must be (2)
      servers.get(0) must be ("failover:(uri1,uri2)?transportOptions&" +
        "nestedURIOptions&soTimeout=15000&soWriteTimeout=15000")
      servers.get(1) must be ("failover:(uri3,uri4)?transportOptions&" +
        "nestedURIOptions&soTimeout=15000&soWriteTimeout=15000")
      }

      {
      val args = Array(
        "failover:(uri1,uri2)",
        "failover:(uri3,uri4)")
      val servers = BalboaJms.parseServers(args, configFromTimeouts())
      servers.size must be (2)
      servers.get(0) must be (
        "failover:(uri1,uri2)?soTimeout=15000&soWriteTimeout=15000")
      servers.get(1) must be (
        "failover:(uri3,uri4)?soTimeout=15000&soWriteTimeout=15000")
      }

      {
      val args = Array(
        "failover:(uri1,uri2)",
        "failover:(uri3,uri4)?soTimeout=15000&soWriteTimeout=15000")
      val servers = BalboaJms.parseServers(args, configFromTimeouts())
      servers.size must be (2)
      servers.get(0) must be (
        "failover:(uri1,uri2)?soTimeout=15000&soWriteTimeout=15000")
      servers.get(1) must be (
        "failover:(uri3,uri4)?soTimeout=15000&soWriteTimeout=15000")
      }
    }

    "read timeouts from config" in {
      val args = Array(
        "failover:(uri1,uri2)?soWriteTimeout=15000",
        "failover:(uri3,uri4)?soTimeout=15000&soWriteTimeout=15000")
      val servers = BalboaJms.parseServers(args,
        configFromTimeouts(soTimeout = 30000))
      servers.size must be (2)
      servers.get(0) must be (
        "failover:(uri1,uri2)?soWriteTimeout=15000&soTimeout=30000")
      servers.get(1) must be (
        "failover:(uri3,uri4)?soTimeout=15000&soWriteTimeout=15000")
    }
  }
}

object BalboaJmsTest extends MockitoSugar {
  val soTimeoutPath = "activemq.sotimeout"
  val soWriteTimeoutPath = "activemq.sowritetimeout"

  def configFromTimeouts(soTimeout: Int = 15000, soWriteTimeout: Int = 15000): Config = {
    val conf = mock[Config]

    def hasPathAndConf(path: String, int: Int) = {
        when(conf.hasPath(path)).thenReturn(true)
        when(conf.getInt(path)).thenReturn(int)
    }

    hasPathAndConf(soTimeoutPath, soTimeout)
    hasPathAndConf(soWriteTimeoutPath, soWriteTimeout)

    conf
  }
}
