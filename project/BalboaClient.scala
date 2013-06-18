import sbt._
import Keys._

import sbtassembly.Plugin.AssemblyKeys._
import com.socrata.socratasbt.SocrataSbt._
import SocrataSbtKeys._

object BalboaClient {
  lazy val settings: Seq[Setting[_]] = BuildSettings.buildSettings ++ socrataProjectSettings(assembly = true) ++ Seq(
    crossScalaVersions := Seq("2.8.2", "2.10.1"),
    libraryDependencies <++= (slf4jVersion) { slf4jVersion => Seq(
      "net.sf.opencsv" % "opencsv" % "2.0.1",
      "org.apache.activemq" % "activemq-core" % "5.2.0",
      "com.socrata" %% "socrata-utils" % "0.5.1",
      "log4j" % "log4j" % "1.2.16",
      "org.apache.httpcomponents" % "httpclient" % "4.0.1",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion
    )},
    jarName in assembly <<= name(_ + "-jar-with-dependencies.jar")
  )
}

