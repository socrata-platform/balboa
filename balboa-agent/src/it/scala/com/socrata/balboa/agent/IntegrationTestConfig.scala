package com.socrata.balboa.agent

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}

object IntegrationTestConfig {
  val conf: Config = ConfigFactory.load().getConfig("com.socrata.balboa.agent.it")

  val metricDirectory: File = new File(conf.getString("metric_directory"))
  val activemqServer: String = conf.getString("activemq_server")
  val activemqQueue: String = conf.getString("activemq_queue")
  val agentPid: String = conf.getString("agent_pid")
}
