import sbt._
import Keys._

import com.socrata.cloudbeessbt.SocrataCloudbeesSbt

object BuildSettings {
  val buildSettings: Seq[Setting[_]] = Defaults.defaultSettings ++ SocrataCloudbeesSbt.socrataBuildSettings ++ Seq(
    scalaVersion := "2.10.1",
    fork in test := true,
    javaOptions in test += "-Dsocrata.env=test"
  )
  def projectSettings(assembly: Boolean = false): Seq[Setting[_]] =
    buildSettings ++ SocrataCloudbeesSbt.socrataProjectSettings(assembly)
}
