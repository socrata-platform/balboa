package com.socrata.balboa.agent

import java.util.UUID
import javax.jms._

import com.blist.metrics.impl.queue.MetricFileQueue
import com.socrata.balboa.agent.CustomMatchers.{haveKey, haveKeyValue}
import com.socrata.metrics.{Fluff, MetricIdPart}
import org.apache.activemq.ActiveMQConnectionFactory
import org.json4s.jackson.JsonMethods.parse
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.duration._
import scala.io.Source
import scala.util.Random

class WriteMetricIntegrationTest extends FlatSpec with BeforeAndAfterAll with Matchers {
  val EntityName: String = s"this.is.an.entity.name.${UUID.randomUUID()}"

  val connectionFactory: ActiveMQConnectionFactory = new ActiveMQConnectionFactory(IntegrationTestConfig.activemqServer)
  val connection: Connection = connectionFactory.createConnection()

  override def beforeAll(): Unit = {
    // The metric output directory may not exist.
    IntegrationTestConfig.metricDirectory.mkdirs()
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

  def withConsumer(testCode: MessageConsumer => Any): Unit = {
    // Read from the ActiveMQ queue where the running balboa-agent should have
    // written the metric.
    val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val destination = session.createQueue(IntegrationTestConfig.activemqQueue)
    val consumer: MessageConsumer = session.createConsumer(destination)
    // Before beginning, clear the queue of any existing messages.
    var message: Message = null
    do {
      message = consumer.receive(5.millisecond.toMillis)
      if (message != null) message.acknowledge()
    } while (message != null)

    try {
      testCode(consumer)
    }
    finally {
      consumer.close()
      session.close()
    }
  }

  "Writing a metric" should "forward on" in withConsumer { consumer =>
    val (metricName, metricValue) = createRandomMetric

    val message = consumer.receive(25.second.toMillis)
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

  it should "clean up file descriptors" in withConsumer { consumer =>
    val pid = IntegrationTestConfig.agentPid
    def numMetricFileDescriptors = {
      val proc = Runtime.getRuntime.exec(s"lsof -p $pid")
      Source.fromInputStream(proc.getInputStream)
        .getLines()
        .filter(p => p.contains("metrics2012"))
        .toSeq
        .length
    }

    val startFileDescriptorNum = numMetricFileDescriptors

    createRandomMetric

    val message = consumer.receive(25.second.toMillis)
    message should not be null
    message.acknowledge()

    val endFileDescriptorNum = numMetricFileDescriptors
    endFileDescriptorNum should equal(startFileDescriptorNum)
  }

  private def createRandomMetric = {
    val metricName = s"metric.name.${UUID.randomUUID()}"
    val metricValue = Random.nextInt()

    val metricFileQueue = new MetricFileQueue(IntegrationTestConfig.metricDirectory)
    metricFileQueue.create(new MetricIdPart(EntityName), Fluff(metricName), metricValue)
    metricFileQueue.close()
    (metricName, metricValue)
  }
}
