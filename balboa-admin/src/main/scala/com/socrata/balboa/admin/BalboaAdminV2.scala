package com.socrata.balboa.admin

import com.socrata.balboa.admin.config.enums.Command
import com.socrata.balboa.admin.config.{CLIConfigProvider, BalboaCLIConfig}

import scala.concurrent.Future

/**
  * Another approach to an administrative application
  *
  * Created by michaelhotan on 11/2/15.
  */
object BalboaAdminV2 extends App with CLIConfigProvider {

  val cliConfig: BalboaCLIConfig = parse(args) match {
    case Some(config) => config
    case None => throw new RuntimeException("Illegal Configuration");
  }

  println("Configuration:")
  println(cliConfig)

  val task: Option[Future[Any]] = cliConfig.command match {
    case Command.migrate => ???
    case x => throw new RuntimeException(s"Unsupported Command: $x");

  }

}
