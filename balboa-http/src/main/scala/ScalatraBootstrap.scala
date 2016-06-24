import javax.servlet.ServletContext

import com.socrata.balboa.server.{HealthCheckServlet, MainServlet, MetricsServlet}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.scalatra.LifeCycle
import org.scalatra.metrics.MetricsBootstrap

class ScalatraBootstrap extends LifeCycle with MetricsBootstrap with StrictLogging {
  override def init(context: ServletContext): Unit = {
    logger.info("Assigning servlet handlers.")
    context.mount(new MainServlet, "/")
    context.mount(new MetricsServlet, "/metrics")
    context.mount(new HealthCheckServlet, "/health")
  }
}
