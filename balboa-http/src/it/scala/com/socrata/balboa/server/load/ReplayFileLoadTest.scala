package com.socrata.balboa.server.load

import com.socrata.balboa.server.Config
import io.gatling.core.Predef._
import io.gatling.core.structure.ScenarioBuilder
import io.gatling.http.Predef._

import scala.concurrent.duration._
import scala.io.Source

/**
  * Load test replaying the requests listed in a csv file pointed to by the configs
  */
class ReplayFileLoadTest extends Simulation {

  val httpConf = http.baseURL(Config.Server.toString)

  // Gets load test file to replay from either /balboa-http/src/it/resources or the project root
  val replayFile = Source.fromInputStream(getClass.getResourceAsStream(
      "/" + Config.conf.getString("replay_load_test_file")))

  val replayTimeout = Config.conf.getInt("replay_load_test_timeout_sec")
  val replayMaxUsers = Config.conf.getInt("replay_load_test_max_users")

  val scn: ScenarioBuilder =
    replayFile.getLines
      .foldRight(scenario(s"Replaying log file"))(
        (request, scnAcc) =>
          scnAcc.exec(http(s"GET $request").get(request.stripPrefix("/")))
    )

  setUp(
    scn.inject(
      atOnceUsers(1),
      nothingFor(4.seconds),
      rampUsers(replayMaxUsers) over 20.seconds
    )
  ).protocols(httpConf).maxDuration(replayTimeout)
}
