package com.socrata.balboa.server

import java.net.URL

import com.typesafe.config.{Config => TypesafeConfig, ConfigFactory}

import scala.concurrent.duration._

object Config {
  val conf: TypesafeConfig = ConfigFactory.load().getConfig("com.socrata.balboa.server.it")
  val Server = new URL(s"http://${conf.getString("service_host")}:${conf.getString("service_port")}/")

  val RequestTimeout: Duration = 5 seconds

  println(s"Executing integration tests against '$Server'")
}
