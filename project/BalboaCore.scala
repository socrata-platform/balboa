import sbt._
import Keys._

import com.socrata.socratasbt.SocrataSbt._
import SocrataSbtKeys._

object BalboaCore {
  lazy val settings: Seq[Setting[_]] = BuildSettings.buildSettings ++ socrataProjectSettings() ++ net.virtualvoid.sbt.graph.Plugin.graphSettings ++ Seq(
    libraryDependencies <++= (slf4jVersion) { slf4jVersion =>
      Seq(
        "junit" % "junit" % "4.5" % "test",
        "com.google.protobuf" % "protobuf-java" % "2.3.0",
        "com.google.guava" % "guava" % "12.0",
        "org.codehaus.jackson" % "jackson-core-lgpl" % "1.6.0",
        "org.codehaus.jackson" % "jackson-mapper-lgpl" % "1.6.0",
        "org.slf4j" % "slf4j-api" % slf4jVersion,
        "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
        "commons-collections" % "commons-collections" % "3.1",
        "commons-dbcp" % "commons-dbcp" % "1.2.2",
        "org.perf4j" % "perf4j" % "0.9.13",
        "com.yammer.metrics" % "metrics-core" % "2.1.2",
        "com.netflix.astyanax" % "astyanax" % "1.56.26",
        "log4j" % "log4j" % "1.2.16"
      )
    },
    dependenciesSnippet :=
      <xml.group>
        <exclude org="javax.servlet" module="servlet-api" />
        <exclude org="org.codehaus.jackson" module="jackson-mapper-asl" />
        <exclude org="org.codehaus.jackson" module="jackson-core-asl" />
      </xml.group>
  )
}
