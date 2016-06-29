package com.socrata.balboa.util

import org.json4s._
import org.json4s.jackson.JsonMethods.{parse, pretty, render}
import org.scalatest.Matchers._

object TestUtil {
  import scala.language.implicitConversions

  class AssertionJSON(j: => String) {
      def shouldBeJSON(expected: String) = {
        val actualObj = parse(j)
        val expectObj = parse(expected)

        withClue(
          "\nTextual actual:\n\n" + pretty(render(actualObj)) + "\n\n\n" +
          "Textual expected:\n\n" + pretty(render(expectObj)) + "\n\n")
          { actualObj should be (expectObj) }
      }
  }
  implicit def convertJSONAssertion(j: => String): AssertionJSON = new AssertionJSON(j)
}
