package com.socrata.balboa.util

import java.io.IOException

import com.socrata.thirdparty.typesafeconfig.Propertizer
import com.typesafe.config.Config
import org.apache.log4j.PropertyConfigurator

object LoggingConfigurator {

  @throws[IOException]
  def configureLogging(config: Config): Unit = {
    val logProperties = Propertizer.apply("", config)

    System.out.println(logProperties.toString)

    val logLevel = System.getProperty("loglevel")
    if (null != logLevel && // scalastyle:ignore
        (logLevel.equals("DEBUG") ||
          logLevel.equals("INFO") ||
          logLevel.equals("ERROR"))) {
      logProperties.setProperty("log4j.logger.com.socrata.balboa", logLevel)
    } else {
      println("Unable to determine log level from JVM environment, using default") // scalastyle:ignore
    }

    PropertyConfigurator.configure(logProperties)
  }
}
