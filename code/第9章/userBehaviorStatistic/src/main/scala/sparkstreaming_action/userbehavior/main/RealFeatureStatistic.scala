package sparkstreaming_action.userbehavior.main

import scala.collection.mutable.ArrayBuffer

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

import sparkstreaming_action.userbehavior.dao.RedisDao
import sparkstreaming_action.userbehavior.util.Conf

// 主函数入口
object RealFeatureStat {
  def main(args: Array[String]): Unit = {
    val realFeature = new RealFeatureStat
    realFeature.train
  }
}

class RealFeatureStat extends Serializable {
  def constructKV(ssc: StreamingContext) = {
    // Kafka数据流
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> Conf.brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> Conf.group,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Conf.topics, kafkaParams))

    val KV = stream.map(record => {
      val arr = record.value.split(Conf.SEPERATOR)
      (arr(Conf.INDEX_LOG_USER), arr)
    })

    KV
  }

  def createContext = {
    val sc = SparkSession.builder().master(Conf.master).appName("realstat").getOrCreate().sparkContext
    val ssc = new StreamingContext(sc, Seconds(Conf.streamIntervel))
    ssc.checkpoint(Conf.checkpointDir)
    val view = constructKV(ssc)
    val kSeq = view.groupByKey

    kSeq.foreachRDD(rdd =>
      rdd.foreachPartition(it => {
        while (it.hasNext) {
          val buf = new ArrayBuffer[Conf.DT]
          while (it.hasNext && buf.size < Conf.batchSize) {
            val (key, records) = it.next()
            val updateBuf: ArrayBuffer[(Long, Long)] = records.map {
              case record =>
                val item = record(Conf.INDEX_LOG_ITEM).toLong
                val time = record(Conf.INDEX_LOG_TIME).toLong
                (item, time)
            }(scala.collection.breakOut)
            buf += ((key, updateBuf))
          }
          RedisDao.updateRedis(buf)
        }
      }))

    ssc
  }

  def train {
    val ssc = StreamingContext.getOrCreate(Conf.checkpointDir, createContext _)
    ssc.start()
    ssc.awaitTermination()

  }
}

