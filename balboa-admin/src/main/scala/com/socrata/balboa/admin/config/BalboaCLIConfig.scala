package com.socrata.balboa.admin.config

import java.util.Date

import com.socrata.balboa.admin.config.enums.Command
import com.socrata.balboa.admin.config.enums.Command.Command
import com.socrata.balboa.metrics.data.Period

/**
  * Command Line configuration Object
  *
  * @param command
  * @param start
  * @param end
  * @param source
  * @param destination
  */
case class BalboaCLIConfig(command: Command = Command.none,
                          start: Option[Date] = None,
                          end: Option[Date] = None,
                          source: Option[String] = None,
                          destination: Option[String] = None,
                          granularity: Option[Period] = None)
