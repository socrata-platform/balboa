import sbt._
import Keys._

import com.socrata.cloudbeessbt.SocrataCloudbeesSbt._
import SocrataSbtKeys._

object BalboaCommon {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    libraryDependencies ++= Seq(
        "junit" % "junit" % "4.5" % "test",
        "com.google.protobuf" % "protobuf-java" % "2.3.0",
        "commons-logging" % "commons-logging" % "1.1.1",
        "org.codehaus.jackson" % "jackson-core-asl" % "1.9.12",
        "org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.12"
    )
  )
}
