package com.socrata.balboa.impl

import java.io.File

import com.socrata.balboa.common.util.MetricsTestStuff
import com.socrata.balboa.impl.BalboaDispatcherComponentSetup.DispatcherSetup
import com.socrata.balboa.metrics.Message
import com.socrata.metrics.components.{EmergencyFileWriterComponent, MessageQueueComponent}
import org.scalatest.{BeforeAndAfter, WordSpec}

import scala.collection.mutable
import scala.collection.mutable.Queue


object BalboaDispatcherComponentSetup {
  
  trait DispatcherSetup {
    val fakeComponents: Seq[FakeQueueComponent] = Seq(
      FakeQueueComponent(), FakeQueueComponent(), FakeQueueComponent())
    val fullDispatcherComponent = new BalboaDispatcherComponent with QueueEmergencyWriter with DispatcherInformation {
      override lazy val components: Iterable[MessageQueueComponent] = fakeComponents
    }
  }
  
}

/**
 * Unit test for [[BalboaDispatcherComponent]].
 */
class BalboaDispatcherComponentSpec extends WordSpec with BeforeAndAfter {

  override protected def before(fun: => Any): Unit = super.before(fun)

  "A BalboaDispatcherComponent" should {

    "start each internal queue component" in new DispatcherSetup {
      val q = fullDispatcherComponent.MessageQueue()
      q.start()
      assert(fakeComponents.forall(_.started))
    }

    "stop each internal queue component when dispatcher stops" in new DispatcherSetup {
      val q = fullDispatcherComponent.MessageQueue()
      q.stop()
      assert(fakeComponents.forall(_.stopped))
    }

    "dispatch a single metrics message to all internal components" in new DispatcherSetup with MetricsTestStuff
    .TestMessages {
      val m = fullDispatcherComponent.MessageQueue()
      m.send(oneElemMessage)
      assert(fakeComponents.forall(_.queue.size == 1))
    }

    "dispatch multiple messages to all internal components" in new DispatcherSetup with MetricsTestStuff.TestMessages {
      val m = fullDispatcherComponent.MessageQueue()
      for (i <- 1 to 10) {
        m.send(oneElemMessage)
      }
      assert(fakeComponents.forall(_.queue.size == 10))
    }
  }
  
}

/**
 * Tracking emergencies locally.
 */
trait QueueEmergencyWriter extends EmergencyFileWriterComponent {

  class EmergencyFileWriter(file:File) extends EmergencyFileWriterLike {
    val emergencyQueue = Queue.empty[Message]
    // Store all files written as emergency
    override def send(msg: Message): Unit = emergencyQueue.enqueue(msg)
    override def close(): Unit = { /** NOOP */ }
  }

  override def EmergencyFileWriter(file: File): EmergencyFileWriter = new EmergencyFileWriter(file)
}

case class FakeQueueComponent() extends MessageQueueComponent {

  val queue = mutable.Queue.empty[Message]
  var started = false
  var stopped = false

  class FakeQueue extends MessageQueueLike {

    /**
     * Initialize the message queue and prepares to recieve messages.
     */
    override def start(): Unit = started = true

    /**
     * Stops and destroys the underlying queue.
     */
    override def stop(): Unit = stopped = true

    /**
     * Sends a message using the underlying queue.
     * @param msg Messsage to send.  Should not be null.
     */
    override def send(msg: Message): Unit = queue.enqueue(msg)
  }

  override def MessageQueue(): MessageQueueLike = new FakeQueue

}
