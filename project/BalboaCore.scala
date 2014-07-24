import sbt._
import Keys._

import com.socrata.cloudbeessbt.SocrataCloudbeesSbt._
import SocrataSbtKeys._

object BalboaCore {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ net.virtualvoid.sbt.graph.Plugin.graphSettings ++ Seq(
    libraryDependencies ++= Seq(
        "com.netflix.astyanax" % "astyanax" % "1.56.26"
      ),
    dependenciesSnippet :=
      <xml.group>
        <exclude org="javax.servlet" module="servlet-api" />
      </xml.group>
  )
}
