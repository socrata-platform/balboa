package com.socrata.balboa.agent

import java.net.URL
import java.util.UUID

import org.joda.time.Duration
import org.json4s.JsonAST.JLong
import org.json4s.jackson.JsonMethods.{pretty, render}
import org.json4s.{JBool, JDouble, JInt, JNothing, JString, JValue}
import org.scalatest.matchers._

trait CustomMatchers {
  /**
    * Make a custom matcher to see if a key exists in a Json object:
    * {{{
    *       jsonResult should haveKey("name")
    * }}}
    */
  class HaveKey(expectedKey: String) extends Matcher[JValue] {
    def apply(left: JValue): MatchResult = {
      MatchResult(
        (left \ expectedKey) != JNothing,
        s"The key '$expectedKey' does not exist in the JSON object '${pretty(render(left))}'",
        s"The key '$expectedKey' does exist in the JSON object."
      )
    }
  }

  def haveKey(expectedKey: String): HaveKey = new HaveKey(expectedKey)

  /**
    * Make a custom matcher to see if a key exists and has the matching value
    * in a Json object:
    * {{{
    *       jsonResult should haveKeyValue("key-name", "expected-value")
    * }}}
    */
  class HaveKeyValue(expectedKey: String, expectedValue: JValue) extends Matcher[JValue] {
    def apply(left: JValue): MatchResult = {
      if ((left \ expectedKey) == JNothing) {
        MatchResult(
          matches = false,
          s"The key '$expectedKey' does not exist in the JSON object '${pretty(render(left))}'",
          s"The key '$expectedKey' does exist in the JSON object."
        )
      } else {
        MatchResult(
          (left \ expectedKey) == expectedValue,
          s"The value for key '$expectedKey' is '${left \ expectedKey}' not '$expectedValue'.",
          s"The key '$expectedKey' have value '${left \ expectedKey}'."
        )
      }
    }
  }

  def haveKeyValue(expectedKey: String, expectedValue: String): HaveKeyValue = new HaveKeyValue(expectedKey, JString(expectedValue))
  def haveKeyValue(expectedKey: String, expectedValue: Int): HaveKeyValue = new HaveKeyValue(expectedKey, JInt(expectedValue))
  def haveKeyValue(expectedKey: String, expectedValue: Long): HaveKeyValue = new HaveKeyValue(expectedKey, JLong(expectedValue))
  def haveKeyValue(expectedKey: String, expectedValue: Double): HaveKeyValue = new HaveKeyValue(expectedKey, JDouble(expectedValue))
  def haveKeyValue(expectedKey: String, expectedValue: Boolean): HaveKeyValue = new HaveKeyValue(expectedKey, JBool(expectedValue))
  def haveKeyValue(expectedKey: String, expectedValue: Duration): HaveKeyValue = new HaveKeyValue(expectedKey, JInt(expectedValue.getMillis))
  def haveKeyValue(expectedKey: String, expectedValue: URL): HaveKeyValue = new HaveKeyValue(expectedKey, JString(expectedValue.toString))
  def haveKeyValue(expectedKey: String, expectedValue: UUID): HaveKeyValue = new HaveKeyValue(expectedKey, JString(expectedValue.toString))
}

/** Allow the custom matchers easy to be imported easily:
  * {{{
  *    import com.socrata.balboa.agent.CustomMatchers._
  * }}}
  */
object CustomMatchers extends CustomMatchers
