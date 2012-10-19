import sbt._
import Keys._

import com.socrata.socratasbt.SocrataSbt._
import com.socrata.socratasbt.CheckClasspath

object BuildSettings {
  val buildSettings: Seq[Setting[_]] = Defaults.defaultSettings ++ socrataBuildSettings ++ Seq(
    artifactName := { (_: ScalaVersion, _: ModuleID, artifact: Artifact) => artifact.name + "." + artifact.extension },
    fork in test := true,
    javaOptions in test += "-Dsocrata.env=test"
  )
}
