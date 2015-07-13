scalaVersion := "2.10.4"

maintainer := "Socrata SUM Team, sum-team-l@socrata.com"

packageSummary := "Balboa"

packageDescription :=
  """
    |Internal Metrics System.
  """.stripMargin

debianPackageDependencies in Debian := Seq("openjdk-7-jre-headless")