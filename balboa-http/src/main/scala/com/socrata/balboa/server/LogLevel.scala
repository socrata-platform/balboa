package com.socrata.balboa.server

import java.util.Properties

import org.apache.log4j.PropertyConfigurator

object LogLevel {
  def configureForMainClass[T](mainClass: Class[T]): Unit = {
    val p = new Properties()
    p.load(mainClass.getClassLoader().getResourceAsStream("config/config.properties"))

    val logLevel = System.getProperty("loglevel")
    if (null != logLevel && (logLevel.equals("DEBUG") || logLevel.equals("INFO") || logLevel.equals("ERROR"))) {
      p.setProperty("log4j.logger.com.socrata.balboa", logLevel)
    } else {
      println("Unable to determine log level from environment, using default")
    }
    PropertyConfigurator.configure(p)
  }
}
