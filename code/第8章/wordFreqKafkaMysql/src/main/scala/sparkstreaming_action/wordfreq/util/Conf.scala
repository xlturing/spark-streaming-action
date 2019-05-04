package sparkstreaming_action.wordfreq.util

import java.net.InetAddress

/**
 * @author litaoxiao
 * configuration
 */
object Conf extends Serializable {
  // parameters configuration
  val nGram = 3
  val updateFreq = 300000 //5min

  // api configuration
  val segmentorHost = "http://localhost:8282"

  // spark configuration
  val master = "spark://localhost:7077"
  val localDir = "/Users/xiaolitao/Program/scala/data/tmp"
  val perMaxRate = "5"
  val interval = 3 // seconds
  val parallelNum = "15"
  val executorMem = "1G"
  val concurrentJobs = "5"
  val coresMax = "3"

  // kafka configuration
  val brokers = "localhost:9091,localhost:9092"
  val zk = "localhost:2181"
  val group = "wordFreqGroup"
  val topics = "test"

  // mysql configuration
  val mysqlConfig = Map("url" -> "jdbc:mysql://localhost:3306/word_freq?characterEncoding=UTF-8", "username" -> "root", "password" -> "root")
  val maxPoolSize = 5
  val minPoolSize = 2
}
