import sbt._
import Keys._

import com.socrata.socratasbt.SocrataSbt._
import SocrataSbtKeys._

object BalboaCore {
  lazy val settings: Seq[Setting[_]] = BuildSettings.buildSettings ++ socrataProjectSettings() ++ Seq(
    libraryDependencies ++= Seq(
      "junit" % "junit" % "4.5" % "test",
      "com.google.protobuf" % "protobuf-java" % "2.3.0",
      "org.codehaus.jackson" % "jackson-core-lgpl" % "1.6.0",
      "org.codehaus.jackson" % "jackson-mapper-lgpl" % "1.6.0",
      "org.slf4j" % "slf4j-api" % "1.6.1",
      "commons-logging" % "commons-logging" % "1.1.1",
      "commons-collections" % "commons-collections" % "3.1",
      "commons-dbcp" % "commons-dbcp" % "1.2.2",
      "org.perf4j" % "perf4j" % "0.9.13",
      "com.yammer" % "metrics_2.8.1" % "2.0.0-BETA10",
      "log4j" % "log4j" % "1.2.16"
    ),
    dependenciesSnippet :=
      <xml.group>
        <exclude org="org.codehaus.jackson" module="jackson-mapper-asl" />
        <exclude org="org.codehaus.jackson" module="jackson-core-asl" />
      </xml.group>
  )
}
