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
    val activemq = "5.2.0"
    val astyanax = "1.56.26"
    val commons_logging = "1.1"
    val jackson = "1.9.12"
    val junit = "4.5"
    val jopt_simple = "4.8"
    val kafka = "0.8.2.0"
    val log4j = "1.2.17"
    val mockito = "1.+"
    val newman = "1.3.5"
    val opencsv = "2.0.1"
    val protobuf_java = "2.3.0"
    val rojoma_json = "[3.3.0, 4.0.0)"
    val scalatest = "[1.8, 1.9.1)"
    val scalatra_scalatest = "2.2.0"
    val scala_logging = "2.1.2"
    val slf4j_log4j = "1.7.12"
    val scopt = "3.3.0"
    val simple_arm = "[1.1.10, 2.0.0)"
    val socrata_http = "1.3.3"
    val socrata_utils = "[0.6.0, 1.0.0)"
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

  val activemq = "org.apache.activemq" % "activemq-core" % versions.activemq
  val astyanax = "com.netflix.astyanax" % "astyanax" % versions.astyanax
  val jackson_core_asl = "org.codehaus.jackson" % "jackson-core-asl" % versions.jackson
  val jackson_mapper_asl = "org.codehaus.jackson" % "jackson-mapper-asl" % versions.jackson
  val junit = "junit" % "junit" % versions.junit % "test"
  val jopt_simple = "net.sf.jopt-simple" % "jopt-simple" % versions.jopt_simple
  val kafka = "org.apache.kafka" %% "kafka" % versions.kafka
  val kafka_test = kafka % "test" classifier "test"
  val mockito_test = "org.mockito" % "mockito-core" % versions.mockito % "test"
  // Functionally Nice REST Client Currently newman only supports scala version 2.9.2 and 2.10
  val newman = "com.stackmob" %% "newman" % versions.newman
  val opencsv = "net.sf.opencsv" % "opencsv" % versions.opencsv
  val protobuf_java = "com.google.protobuf" % "protobuf-java" % versions.protobuf_java
  val rojoma_json = "com.rojoma" %% "rojoma-json-v3" % versions.rojoma_json
  val scala_test = "org.scalatest" %% "scalatest" % versions.scalatest % "test"
  val scalatra_scalatest = "org.scalatra" %% "scalatra-scalatest" % versions.scalatra_scalatest % "test"
  val scopt = "com.github.scopt" %% "scopt" % versions.scopt
  val simple_arm = "com.rojoma" %% "simple-arm" % versions.simple_arm
  val socrata_http = "com.socrata" %% "socrata-http" % versions.socrata_http excludeAll
    ExclusionRule("org.eclipse.jetty.orbit", "javax.servlet", configurations = Seq("test"))
  val socrata_utils = "com.socrata" %% "socrata-utils" % versions.socrata_utils


}
