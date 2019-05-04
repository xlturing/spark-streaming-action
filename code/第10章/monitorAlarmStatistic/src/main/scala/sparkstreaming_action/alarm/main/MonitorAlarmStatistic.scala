package sparkstreaming_action.alarm.main

import java.util.Properties

import scala.collection.mutable.Map

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

import sparkstreaming_action.alarm.dao.KafkaSink
import sparkstreaming_action.alarm.entity.MonitorGame
import sparkstreaming_action.alarm.service.MysqlService
import sparkstreaming_action.alarm.service.SegmentService
import sparkstreaming_action.alarm.util.BroadcastWrapper
import sparkstreaming_action.alarm.util.Conf

object MonitorAlarmStatistic {
  @transient lazy val log = LogManager.getRootLogger
  def createContext = {
    val sparkConf = new SparkConf().setAppName("MonitorAlarm").setMaster(Conf.master)
      .set("spark.default.parallelism", Conf.parallelNum)
      .set("spark.streaming.concurrentJobs", Conf.concurrentJobs)
      .set("spark.executor.memory", Conf.executorMem)
      .set("spark.cores.max", Conf.coresMax)
      .set("spark.local.dir", Conf.localDir)
      .set("spark.streaming.kafka.maxRatePerPartition", Conf.perMaxRate)
    val ssc = new StreamingContext(sparkConf, Seconds(Conf.interval))

    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> Conf.brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> Conf.group,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))
    val kafkaDirectStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Conf.topics, kafkaParams))
    log.warn(s"Initial Done>>>topic:${Conf.topics}   group:${Conf.group} brokers:${Conf.brokers}")

    // 广播监控游戏库
    val MonitorGame = BroadcastWrapper[(Long, Map[Int, MonitorGame])](ssc, (System.currentTimeMillis, MysqlService.getGames))
    // 广播KafkaSink
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", Conf.brokers)
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
      log.warn("kafka producer init done!")
      ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

    //经过分词得到新的stream
    val segmentedStream = kafkaDirectStream.map(_.value).transform(rdd => {
      //定期更新监控游戏库
      if (System.currentTimeMillis - MonitorGame.value._1 > Conf.updateFreq) {
        MonitorGame.update((System.currentTimeMillis, MysqlService.getGames), true)
        log.warn("[BroadcastWrapper] MonitorGame updated")
      }
      rdd.flatMap(json => SegmentService.mapSegment(json, MonitorGame.value._2))
    })

    //输出到kafka
    segmentedStream.foreachRDD(rdd => {
      if (!rdd.isEmpty) {
        rdd.foreach(record => {
          kafkaProducer.value.send(Conf.outTopics, record._1.toString, record._2)
          log.warn(s"[kafkaOutput] output to ${Conf.outTopics} gameId: ${record._1}")
        })
      }
    })
    ssc
  }

  def main(args: Array[String]) {
    // 因为有广播变量无法使用Checkpointing
    val ssc = createContext
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}