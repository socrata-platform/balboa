import Dependencies._
import com.socrata.cloudbeessbt.SocrataCloudbeesSbt.SocrataSbtKeys._
import sbt.Keys._
import sbt._

object BalboaCore {

  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ net.virtualvoid.sbt.graph.Plugin.graphSettings ++ Seq(
    libraryDependencies <++= scalaVersion {libraries(_)},
    dependenciesSnippet :=
      <xml.group>
        <exclude org="javax.servlet" module="servlet-api" />
      </xml.group>
  )

  def libraries(implicit scalaVersion: String) = Seq(
    junit,
    astyanax
  )
}
