import sbt.Keys._
import sbt._

object BuildSettings {
  val buildSettings: Seq[Setting[_]] = Defaults.coreDefaultSettings++ Seq(
    // TODO: enable style build failures
    com.socrata.sbtplugins.StylePlugin.StyleKeys.styleFailOnError in Compile := false,
    // TODO: enable coverage build failures
    scoverage.ScoverageSbtPlugin.ScoverageKeys.coverageFailOnMinimum := false,
    // TODO: enable findbugs build failures
    com.socrata.sbtplugins.findbugs.JavaFindBugsPlugin.JavaFindBugsKeys.findbugsFailOnError in Compile := false,
    com.socrata.sbtplugins.findbugs.JavaFindBugsPlugin.JavaFindBugsKeys.findbugsFailOnError in Test := false,
    fork in test := true,
    javaOptions in test += "-Dsocrata.env=test",
    scalacOptions in (Compile, doc) ++= Seq( // Related Issue: http://scala-language.1934581.n4.nabble.com/Scaladoc-2-11-quot-throws-tag-quot-cannot-find-any-member-to-link-td4641850.html
      "-no-link-warnings" // Suppresses problems with Scaladoc @throws links
    )
    )
  val projectSettings: Seq[Setting[_]] = buildSettings
}
