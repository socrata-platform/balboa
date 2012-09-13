import sbt._
import Keys._
import com.github.siasia.WebPlugin._
import com.github.siasia.PluginKeys._

import com.socrata.socratasbt.SocrataSbt._
import SocrataSbtKeys._

object BalboaHttp {
  lazy val settings: Seq[Setting[_]] = BuildSettings.buildSettings ++ webSettings ++ socrataProjectSettings() ++ Seq(
    libraryDependencies ++= Seq(
      "org.mortbay.jetty" % "jetty" % "6.1.22" % "container",
      "com.sun.jersey" % "jersey-server" % "1.4",
      "javax.servlet" % "servlet-api" % "2.4" % "provided",
      "junit" % "junit" % "4.5" % "test"
    ),
    port in container.Configuration := 9898
  )
}
