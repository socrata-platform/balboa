import javax.servlet.ServletContext

import com.socrata.balboa.server.MainServlet
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.scalatra.LifeCycle

class ScalatraBootstrap extends LifeCycle with StrictLogging {
  override def init(context: ServletContext): Unit = {
    logger.info("Assigning servlet handlers.")
    context.mount(new MainServlet, "/")
  }
}
