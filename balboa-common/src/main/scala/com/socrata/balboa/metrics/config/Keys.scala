package com.socrata.balboa.metrics.config

/**
 * Reference to all Property Keys for Balboa.  This prevents naming collisions
 * and potential type errors.  It also can serve as a single source reference for
 * what the keys are intended to be referencing.
 */
object Keys {

  val RootNamespace = join("balboa") _

  val RootDispatcherNamespace = join(RootNamespace("dispatcher")) _
  val RootJMSNamespace = join(RootNamespace("jms")) _
  val RootAgentNamespace = join(RootNamespace("agent")) _
  val RootJMSActiveMQNamespace = join(RootJMSNamespace("activemq")) _
  val RootEmergencyDirNamespace = join(RootNamespace("emergency")) _

  val JMSActiveMQServer = RootJMSActiveMQNamespace("server")
  val JMSActiveMQQueue = RootJMSActiveMQNamespace("queue")
  val JMSActiveMQUser = RootJMSActiveMQNamespace("user")
  val JMSActiveMQPassword = RootJMSActiveMQNamespace("password")
  val JMSActiveMQMaxBufferSize = RootJMSActiveMQNamespace("buffer.size")
  val JMSActiveMQThreadsPerServer = RootJMSActiveMQNamespace("threads-per-server")

  val DataDirectory = RootAgentNamespace("data.dir")
  val SleepMs = RootAgentNamespace("sleeptime")
  val InitialDelayMs = RootAgentNamespace("initialdelay.ms")
  val TransportType = RootAgentNamespace("transport.type")
  val BalboaHttpUrl = RootAgentNamespace("balboa.http.url")
  val BalboaHttpTimeoutMs = RootAgentNamespace("balboa.http.timeout.ms")
  val BalboaHttpMaxRetryWaitMs = RootAgentNamespace("balboa.http.max.retry.wait.ms")

  val BackupDir = RootEmergencyDirNamespace("backup.dir")

  val DispatcherClientTypes = RootDispatcherNamespace("types")

  def join(parent: String)(child: String): String = parent + "." + child
}
