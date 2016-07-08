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

  val replayRows = replayFile.getLines.drop(1)
    .map(_.split(",").map(_.trim)) // Break CSV into columns
    .map(row => (row(0).toInt, row(1))) // Array[String] => Tuple(millisecond request time as int, request)
    .toList
    .sortWith(_._1 < _._1) // Sort by request time

  val scn: ScenarioBuilder =
    replayRows.zipWithIndex.foldRight(scenario(s"Replaying log file"))({
      case (((time, request), index), scnAcc) =>
        if (index == 0) {
          scnAcc
        } else {
          val timeAfterLast = time - replayRows(index - 1)._1
          scnAcc.pause(timeAfterLast.millis)
        }.exec(http(s"GET $request").get(request.stripPrefix("/")))
    })

  setUp(
    scn.inject(atOnceUsers(1))
  ).protocols(httpConf).maxDuration(40.seconds)
}
