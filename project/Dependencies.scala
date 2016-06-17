import sbt._

/**
 * Singleton Container for the entire builds dependencies and versions.
 * <br>
 *   Consolidation of the library and versions aids in preventing dependency conflicts.
 */
object Dependencies {

  /**
   * External Library versions.
   */
  private object versions {
    val astyanax = "1.56.26"
    val commons_logging = "1.1"
    val datastax = "3.0.0"
    val dropwizard = "3.1.2"
    val jackson = "1.9.12"
    val jetty_webapp = "9.2.14.v20151106"
    val junit = "4.5"
    val jopt_simple = "4.8"
    val kafka = "0.8.2.0"
    val log4j = "1.2.17"
    val mockito = "1.+"
    val newman = "1.3.5"
    val opencsv = "2.0.1"
    val protobuf_java = "2.3.0"
    val rojoma_json = "[2.1.0, 3.0.0)"
    val scala_logging = "2.1.2"
    val slf4j_log4j = "1.7.12"
    val scopt = "3.3.0"
    val simple_arm = "[1.1.10, 2.0.0)"
    val socrata_utils = "[0.6.0, 1.0.0)"
    val scalatra = "2.4.0"
  }

  //////////////////////////////////////////////////////////////////
  // Library dependencies for all Balboa Projects.
  //////////////////////////////////////////////////////////////////

  // Logging Abstraction Layer for Scala and Java.  Using SLF4J
  val scala_logging = "com.typesafe.scala-logging" %% "scala-logging-slf4j" % versions.scala_logging
  // Logging SLF4J binding to Log4J
  val slf4j_log4j = "org.slf4j" % "slf4j-log4j12" % versions.slf4j_log4j
  // Underlying Log4J library
  val log4j = "log4j" % "log4j" % versions.log4j

  val balboa_logging = Seq(scala_logging, slf4j_log4j, log4j)

  val activemq = "org.apache.activemq" % "activemq-client" % "5.13.3"
  // This library is used at runtime for making connections to certain versions
  // of ActiveMQ. Removing this dependency will not cause compile problems, but
  // will show up as runtime problems and only be logged correctly when the AMQ
  // library is at log level DEBUG.
  val activemqOpenwire = "org.apache.activemq" % "activemq-openwire-legacy" % "5.13.3"
  val commons_logging = "commons-logging" % "commons-logging" % versions.commons_logging
  val cassandra_driver_core = "com.datastax.cassandra" %% "cassandra-driver-core" % versions.datastax
  val cassandra_driver_mapping = "com.datastax.cassandra" %% "cassandra-driver-mapping" % versions.datastax
  val cassandra_driver_extras = "com.datastax.cassandra" %% "cassandra-driver-extras" % versions.datastax
  val dropwizard_metrics = "io.dropwizard.metrics" % "metrics-core" % versions.dropwizard
  val dropwizard_healthcheck = "io.dropwizard.metrics" % "metrics-healthchecks" % versions.dropwizard
  val dropwizard_servlets = "io.dropwizard.metrics" % "metrics-servlets" % versions.dropwizard
  val jackson_core_asl = "org.codehaus.jackson" % "jackson-core-asl" % versions.jackson
  val jackson_mapper_asl = "org.codehaus.jackson" % "jackson-mapper-asl" % versions.jackson
  val jetty_webapp = "org.eclipse.jetty" % "jetty-webapp" % versions.jetty_webapp % "compile;container"
  val junit = "junit" % "junit" % versions.junit % "test"
  val jopt_simple = "net.sf.jopt-simple" % "jopt-simple" % versions.jopt_simple
  val json4s = "org.json4s" %% "json4s-jackson" % "3.3.0"
  val kafka = "org.apache.kafka" %% "kafka" % versions.kafka
  val kafka_test = kafka % "test" classifier "test"
  val mockito_test = "org.mockito" % "mockito-core" % versions.mockito % "test"
  // Functionally Nice REST Client Currently newman only supports scala version 2.9.2 and 2.10
  val newman = "com.stackmob" %% "newman" % versions.newman
  val opencsv = "net.sf.opencsv" % "opencsv" % versions.opencsv
  val protobuf_java = "com.google.protobuf" % "protobuf-java" % versions.protobuf_java
  val rojoma_json = "com.rojoma" %% "rojoma-json" % versions.rojoma_json
  val scopt = "com.github.scopt" %% "scopt" % versions.scopt
  val simple_arm = "com.rojoma" %% "simple-arm" % versions.simple_arm
  val socrata_utils = "com.socrata" %% "socrata-utils" % versions.socrata_utils
  val scalatest = "org.scalatest" %% "scalatest" % "2.2.4" % "test,it"
  val scalatra = "org.scalatra" %% "scalatra" % versions.scalatra
  val scalatra_json = "org.scalatra" %% "scalatra-json" % versions.scalatra
  val typesafe_config = "com.typesafe" % "config" % "1.2.1" % "it"
}
