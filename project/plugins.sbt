resolvers := Seq(
  "socrata maven" at "https://repo.socrata.com/artifactory/libs-release/",
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots"),
  Resolver.bintrayRepo("scalaz", "releases")
)

addSbtPlugin("com.socrata" % "socrata-sbt-plugins" % "1.6.2")
addSbtPlugin("org.scalatra.sbt" % "scalatra-sbt" % "0.5.0")
addSbtPlugin("com.earldouglas" % "xsbt-web-plugin" % "2.1.0")
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.0.6")
addSbtPlugin("io.gatling" % "gatling-sbt" % "2.2.0")
