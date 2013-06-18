import sbt._
import Keys._

import com.socrata.socratasbt.SocrataSbt._
import com.socrata.socratasbt.CheckClasspath

object BuildSettings {
  val buildSettings: Seq[Setting[_]] = Defaults.defaultSettings ++ socrataBuildSettings ++ Seq(
    scalaVersion := "2.10.1",
    fork in test := true,
    javaOptions in test += "-Dsocrata.env=test"
  )
}
