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
    val commons_logging = "1.1.1"
    val jackson = "1.9.12"
    val junit = "4.5"
    val kafka = "[0.8.2.1, 0.9.0.0)"
    val log4j = "1.2.16"
    val mockito = "1.+"
    val newman = "1.3.5"
    val opencsv = "2.0.1"
    val protobuf_java = "2.3.0"
    val rojoma_json = "[2.1.0, 3.0.0)"
    val scalatest = "[1.8, 1.9.1)"
    val scopt = "3.3.0"
    val simple_arm = "[1.1.10, 2.0.0)"
    val socrata_http = "1.3.3"
    val socrata_utils = "[0.6.0, 1.0.0)"
  }

  // Library dependencies for the entire project.

  val activemq = "org.apache.activemq" % "activemq-core" % versions.activemq
  val astyanax = "com.netflix.astyanax" % "astyanax" % versions.astyanax
  val commons_logging = "commons-logging" % "commons-logging" % versions.commons_logging
  val jackson_core_asl = "org.codehaus.jackson" % "jackson-core-asl" % versions.jackson
  val jackson_mapper_asl = "org.codehaus.jackson" % "jackson-mapper-asl" % versions.jackson
  val junit = "junit" % "junit" % versions.junit % "test"
  val kafka = "org.apache.kafka" %% "kafka" % versions.kafka
  val kafka_test = kafka % "test" classifier "test"
  val log4j = "log4j" % "log4j" % versions.log4j
  val mockito = "org.mockito" % "mockito-core" % versions.mockito % "test"
  // Functionally Nice REST Client Currently newman only supports scala version 2.9.2 and 2.10
  val newman = "com.stackmob" %% "newman" % versions.newman
  val opencsv = "net.sf.opencsv" % "opencsv" % versions.opencsv
  val protobuf_java = "com.google.protobuf" % "protobuf-java" % versions.protobuf_java
  val rojoma_json = "com.rojoma" %% "rojoma-json" % versions.rojoma_json
  val scalatest = "org.scalatest" %% "scalatest" % versions.scalatest % "test"
  val scopt = "com.github.scopt" %% "scopt" % versions.scopt
  val simple_arm = "com.rojoma" %% "simple-arm" % versions.simple_arm
  val socrata_http = "com.socrata" %% "socrata-http" % versions.socrata_http
  val socrata_utils = "com.socrata" %% "socrata-utils" % versions.socrata_utils


}
