package com.socrata.metrics.impl

/**
 * Information that pertains of how to setup and configure Kafka
 */
trait KafkaInformation {

  /**
   * @return List of Kafka Brokers that exists within this environment
   */
  def brokers: List[AddressAndPort]

  /**
   * @return The
   */
  def topic: String

}
case class Cons(brokers: List[AddressAndPort], topic: String) extends KafkaInformation

/**
 * Kafka Information.
 */
object KafkaInformation {

  /**
   * Attempts to create Kafka information.  If an empty list (aka Nil) is used then this will try to contact
   * consul to retrieve any registered kafka-brokers.  If the request fails then Left(Error message) is returned otherwise
   * Right(KafkaInformation) is returned.  If a non empty list is returned then it is up to the caller to verify the
   * address list is composed of valid inetaddresses.  Non empty list will always return Right(KafkaInformation)
   *
   * @param topic Kafka topic.
   * @param brokers Kafka brokers to connect to.
   * @return
   */
  def cons(topic:String, brokers: List[AddressAndPort] = Nil): Either[String, KafkaInformation] =
    brokers match {
      case Nil => ??? // TODO Knock this out.
      case list => Right(Cons(brokers, topic))
    }
}
