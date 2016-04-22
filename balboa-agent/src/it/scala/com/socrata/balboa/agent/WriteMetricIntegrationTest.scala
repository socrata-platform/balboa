package com.socrata.balboa.agent

import java.util.UUID
import javax.jms.{Message, TextMessage, Connection, ExceptionListener, JMSException, Session}

import com.blist.metrics.impl.queue.MetricFileQueue
import com.socrata.balboa.agent.CustomMatchers.{haveKeyValue, haveKey}
import com.socrata.metrics.{Fluff, MetricIdPart}
import org.apache.activemq.ActiveMQConnectionFactory
import org.json4s.jackson.JsonMethods.parse
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.duration._
import util.Random

class WriteMetricIntegrationTest extends FlatSpec with BeforeAndAfterAll with Matchers {
  val EntityName: String = s"this.is.an.entity.name.${UUID.randomUUID()}"

  val connectionFactory: ActiveMQConnectionFactory = new ActiveMQConnectionFactory(Config.activemqServer)
  val connection: Connection = connectionFactory.createConnection()

  override def beforeAll(): Unit = {
    // The metric output directory may not exist.
    Config.metricDirectory.mkdirs()

    connection.setExceptionListener(new ExceptionListener {
      override def onException(exception: JMSException): Unit = {
        println(s"ERROR: There was some kind of problem with the ActiveMQ connection. $exception")
        require(requirement = false, s"There was an exception. $exception")
      }
    })
    connection.start()
  }

  override def afterAll(): Unit = {
    connection.stop()
  }

  "Writing a metric" should "forward on" in {
    // Read from the ActiveMQ queue where the running balboa-agent should have
    // written the metric.
    val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val destination = session.createQueue(Config.activemqQueue)
    val consumer = session.createConsumer(destination)
    // Before beginning, clear the queue of any existing messages.
    var message: Message = null
    do {
      message = consumer.receive(5.millisecond.toMillis)
      if (message != null) message.acknowledge()
    } while (message != null)


    val metricName = s"metric.name.${UUID.randomUUID()}"
    val metricValue = Random.nextInt()

    // Write a metric to where the running balboa-agent instance can see it.
    val metricFileQueue = new MetricFileQueue(Config.metricDirectory)
    metricFileQueue.create(new MetricIdPart(EntityName), Fluff(metricName), metricValue)
    // Close the metric stream will cause the output file to be marked as
    // completed. balboa-agent won't start consuming metrics from a file until
    // it is marked as completed, meaning nothing more will be appended.
    metricFileQueue.close()

    message = consumer.receive(25.second.toMillis)
    message should not be null
    message.acknowledge()

    message shouldBe a [TextMessage]
    val jsonBody = parse(message.asInstanceOf[TextMessage].getText)
    jsonBody should haveKeyValue("entityId", EntityName)
    jsonBody should haveKey("metrics")
    val metricsBody = jsonBody \ "metrics"
    metricsBody should haveKey(metricName)
    val valueBody = metricsBody \ metricName
    valueBody should haveKeyValue("value", metricValue)
  }

}
