package sparkstreaming_action.crawler.util

import java.net.InetAddress

/**
 * @author litaoxiao
 * configuration
 */
object Conf extends Serializable {
  // mysql configuration
  val mysqlConfig = Map("url" -> "jdbc:mysql://localhost:3306/monitor_alarm?characterEncoding=UTF-8", "username" -> "root", "password" -> "root")
  val maxPoolSize = 5
  val minPoolSize = 2
}
