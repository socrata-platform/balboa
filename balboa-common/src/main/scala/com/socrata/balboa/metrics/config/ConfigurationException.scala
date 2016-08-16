package com.socrata.balboa.metrics.config

class ConfigurationException(message: String = null, cause: Throwable = null) // scalastyle:off null
  extends RuntimeException(message, cause)
