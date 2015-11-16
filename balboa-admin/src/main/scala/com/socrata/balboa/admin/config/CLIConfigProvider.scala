package com.socrata.balboa.admin.config

import com.socrata.balboa.admin.config.enums.Command
import com.socrata.balboa.metrics.data.Period


/**
  * Created by michaelhotan on 11/2/15.
  */
trait CLIConfigProvider extends OptionParserProvider {

  def parse(args: Array[String]): Option[BalboaCLIConfig] = parser.parse(args, BalboaCLIConfig())

  def usage = parser.usage

}


/**
  * Class that provides the command line option parser
  */
sealed trait OptionParserProvider extends OptionConstants with OptionHelpers {

  /**
    * Define the Command line Option Parser.
    */
  val parser = new scopt.OptionParser[BalboaCLIConfig](APPLICATION_NAME) {
    head(APPLICATION_NAME, "0.1.*")
    cmd(Command.emit.toString) action { (_, c) => c.copy(command = Command.emit) } text(EMIT_COMMAND)
    cmd(Command.migrate.toString) action { (_, c) => c.copy(command = Command.migrate) } text(MIGRATE_COMMAND) children(
      //opt[String](START_DATE_OPT) action { (x,c) => c.copy(start = Some(x)) } text(START_DATE_DESCRIPTION),
      //opt[Date](END_DATE_OPT) action { (x,c) => c.copy(end = Some(x)) } text(END_DATE_DESCRIPTION),
      opt[String](SOURCE_ENTITY_ID_OPT) action { (x,c) => c.copy(source = Some(x)) } text(SOURCE_ENTITY_ID_DESCRIPTION),
      opt[String](DESTINATION_ENTITY_ID_OPT) action { (x,c) => c.copy(destination = Some(x)) } text(DESTINATION_ENTITY_ID_DESCRIPTION),
      opt[String](GRANULARITY_OPT) action { (x,c) => c.copy(granularity = Some(Period.valueOf(x.toUpperCase))) } text(GRANULARITY_DESCRIPTION)
      )
  }
}

/**
  * Static constants intended for singular reference.
  */
sealed trait OptionConstants {

  final val APPLICATION_NAME = "balboa-admin"
  final val EMIT_COMMAND = "Emit a single metric to an existing Entity ID"
  final val MIGRATE_COMMAND = "Migrate a Entity ID to another Entity ID.  Should only be used by Admin."
  final val START_DATE_OPT = "start"
  final val END_DATE_OPT = "end"
  final val SOURCE_ENTITY_ID_OPT = "source"
  final val DESTINATION_ENTITY_ID_OPT = "dest"
  final val GRANULARITY_OPT = "granularity"
  final val SOURCE_ENTITY_ID_DESCRIPTION = "The source entity id to migrate the metrics from."
  final val DESTINATION_ENTITY_ID_DESCRIPTION = "The destination entity id to migrate the metrics to."
  final val GRANULARITY_DESCRIPTION = "The target granularity."
  final val START_DATE_DESCRIPTION = "The beginning of the date range.  Defaults to time 0."
  final val END_DATE_DESCRIPTION = "The end of the date range.  Defaults to time now."
}

/**
  * Helper methods user to provide a consistent UI.
  */
sealed trait OptionHelpers {

}