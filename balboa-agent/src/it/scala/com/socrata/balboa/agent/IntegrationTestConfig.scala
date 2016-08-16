package com.socrata.balboa.agent

import java.io.File

import com.typesafe.config.ConfigFactory

import scala.language.postfixOps

object IntegrationTestConfig {
  val conf = ConfigFactory.load().getConfig("com.socrata.balboa.agent.it")

  val metricDirectory = new File(conf.getString("metric_directory"))
  val activemqServer = conf.getString("activemq_server")
  val activemqQueue = conf.getString("activemq_queue")
}
