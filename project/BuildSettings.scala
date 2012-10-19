import sbt._
import Keys._

import com.socrata.socratasbt.SocrataSbt._
import com.socrata.socratasbt.CheckClasspath

object BuildSettings {
  val buildSettings: Seq[Setting[_]] = Defaults.defaultSettings ++ socrataBuildSettings ++ Seq(
    testOptions in Test += Tests.Setup( () => sys.props("socrata.env") = "test" ),
    artifactName := { (_: ScalaVersion, _: ModuleID, artifact: Artifact) => artifact.name + "." + artifact.extension },
    parallelExecution in Test := false
  )
}
