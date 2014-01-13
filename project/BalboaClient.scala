import sbt._
import Keys._

import sbtassembly.Plugin.AssemblyKeys._
import com.socrata.socratasbt.SocrataSbt._
import SocrataSbtKeys._

object BalboaClient {
  lazy val settings: Seq[Setting[_]] = BuildSettings.buildSettings ++ socrataProjectSettings(assembly = true) ++ Seq(
    crossScalaVersions := Seq("2.8.2", "2.10.1"),
    libraryDependencies ++= Seq(
      "org.apache.activemq" % "activemq-core" % "5.2.0",
      "com.socrata" %% "socrata-utils" % "[0.6.0,1.0.0)",
      "com.rojoma" %% "simple-arm" % "1.1.10",
      "log4j" % "log4j" % "1.2.16"
    ),
    jarName in assembly <<= name(_ + "-jar-with-dependencies.jar"),
    dependenciesSnippet :=
      <xml.group>
        <conflict org="com.socrata" manager="latest-compatible" />
        <conflict org="com.rojoma" manager="latest-compatible" />
      </xml.group>
  )
}

