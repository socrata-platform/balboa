addSbtPlugin("com.socrata" % "socrata-cloudbees-sbt" % "1.3.4")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.7.4")

// TODO Try this Docker plug in later.
// Why not have SBT call docker cli to build image and post to registry?
// Another process that I feel should be more automated but it is sadly not.
addSbtPlugin("se.marcuslonnberg" % "sbt-docker" % "1.0.0")