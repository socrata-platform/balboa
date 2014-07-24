import sbt._
import Keys._

import sbtassembly.Plugin.AssemblyKeys._
import com.socrata.cloudbeessbt.SocrataCloudbeesSbt._
import SocrataSbtKeys._

object BalboaJms {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly = true) ++ Seq(
    mainClass in assembly := Some("com.socrata.balboa.jms.BalboaJms"),
    libraryDependencies ++= Seq(
      "org.apache.activemq" % "activemq-core" % "5.2.0"
    ),
    dependenciesSnippet :=
      <xml.group>
        <exclude org="commons-logging" module="commons-logging"/>
      </xml.group>
  )
}
