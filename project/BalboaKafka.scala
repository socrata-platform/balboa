import com.socrata.cloudbeessbt.SocrataCloudbeesSbt.SocrataSbtKeys._
import sbt.Keys._
import sbt._

import sbtassembly.Plugin.AssemblyKeys._

object BalboaKafka {

  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly = true) ++ Seq(
    mainClass in assembly := Some("com.socrata.balboa.kafka.Main"),
    libraryDependencies ++= Seq(
      "org.mockito" % "mockito-core" % "1.+" % "test",
      "junit" % "junit" % "4.5" % "test",
      "org.scalatest" %% "scalatest" % "2.2.4" % "test",
      "org.apache.kafka" %% "kafka" % "0.8.2.0",
      "commons-logging" % "commons-logging" % "1.1.1",
      "com.github.scopt" %% "scopt" % "3.3.0"
    )
  )
}