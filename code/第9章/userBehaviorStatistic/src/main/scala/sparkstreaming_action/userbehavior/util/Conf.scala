package sparkstreaming_action.userbehavior.util

import scala.collection.mutable.ArrayBuffer

object Conf {
  type DT = (String, ArrayBuffer[(Long, Long)])

  // Kafka记录
  val INDEX_LOG_TIME = 0
  val INDEX_LOG_USER = 1
  val INDEX_LOG_ITEM = 2
  val SEPERATOR = "\t"

  // 窗口配置
  val INDEX_TIEMSTAMP = 1
  val MAX_CNT = 25
  val EXPIRE_DURATION = 60 * 60 * 24 * 3
  var windowSize = 72 * 3600

  // redis config
  val RECORD_SZ = 2
  var redisIp = "127.0.0.1"
  var redisPort = 6379
  var passwd = ""

  // kafka config
  val brokers = "localhost:9091,localhost:9092"
  val zk = "localhost:2181"
  val group = "UBSGroup"
  val topics = List("userBehavior")

  // spark config
  val master = "spark://localhost:7077"
  val checkpointDir = "/Users/xiaolitao/Program/scala/userBehaviorStatistic/tmp"
  val streamIntervel = 3
  val partitionNumber = 2
  val batchSize = 64
}
