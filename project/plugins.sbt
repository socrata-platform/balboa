resolvers := Seq(
  Resolver.url("socrata", url("https://repo.socrata.com/artifactory/ivy-libs-release/"))(Resolver.ivyStylePatterns),
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots"),
  Resolver.bintrayRepo("scalaz", "releases")
)

addSbtPlugin("com.socrata" % "socrata-sbt-plugins" % "1.6.8")
addSbtPlugin("org.scalatra.sbt" % "scalatra-sbt" % "0.5.0")
addSbtPlugin("com.earldouglas" % "xsbt-web-plugin" % "2.1.0")
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.0.6")
addSbtPlugin("io.gatling" % "gatling-sbt" % "2.2.0")
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.6.1")
