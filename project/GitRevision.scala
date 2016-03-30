import sbt.{Process, ProcessLogger}

/**
  * Object that extracts the full git commit hash.
  *
  * Functionality modelled after https://github.com/socrata-platform/socrata-sbt-plugins.
  */
object GitRevision {

  private[this] class SimpleProcessLog extends ProcessLogger {
    var errorString: Option[String] = None
    var infoString: Option[String] = None
    override def buffer[T](f: => T): T = f
    override def error(s: => String): Unit = errorString = Some(s)
    override def info(s: => String): Unit = infoString = Some(s)
  }

  private[this] val gitCommand = Seq("git", "rev-parse", "HEAD")

  /**
    * @return The current git commit hash of this project
    */
  def gitCommit: String = {
    val procLog = new SimpleProcessLog
    val exitCode = Process(gitCommand).!(procLog)
    exitCode match {
      case 0 => procLog.infoString match {
        case Some(sha) => sha
        case _ => throw new IllegalStateException(s"${gitCommand.mkString(" ")} resolved to no discernible output")
      }
      case n: Int => s"No git commit sha.  Not a git project."
    }
  }

}