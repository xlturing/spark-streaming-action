package sparkstreaming_action.wordfreq.main

import scala.collection.mutable.Map
import scala.collection.mutable.HashSet

import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.apache.spark.streaming._
import spray.json._

import sparkstreaming_action.wordfreq.dao.KafkaManager
import sparkstreaming_action.wordfreq.service.MysqlService
import sparkstreaming_action.wordfreq.service.SegmentService
import sparkstreaming_action.wordfreq.util.BroadcastWrapper
import sparkstreaming_action.wordfreq.util.Conf

/**
 * @author litaoxiao
 *
 */
object ConsumerMain extends Serializable {
  @transient lazy val log = LogManager.getRootLogger
  def functionToCreateContext(): StreamingContext = {
    val sparkConf = new SparkConf().setAppName("WordFreqConsumer").setMaster(Conf.master)
      .set("spark.default.parallelism", Conf.parallelNum)
      .set("spark.streaming.concurrentJobs", Conf.concurrentJobs)
      .set("spark.executor.memory", Conf.executorMem)
      .set("spark.cores.max", Conf.coresMax)
      .set("spark.local.dir", Conf.localDir)
      .set("spark.streaming.kafka.maxRatePerPartition", Conf.perMaxRate)
    val ssc = new StreamingContext(sparkConf, Seconds(Conf.interval))
//    ssc.checkpoint(Conf.localDir)

    // Create direct kafka stream with brokers and topics
    val topicsSet = Conf.topics.split(",").toSet
    val kafkaParams = scala.collection.immutable.Map[String, String]("metadata.broker.list" -> Conf.brokers, "auto.offset.reset" -> "smallest", "group.id" -> Conf.group)
    val km = new KafkaManager(kafkaParams)
    val kafkaDirectStream = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    log.warn(s"Initial Done***>>>topic:${Conf.topics}   group:${Conf.group} localDir:${Conf.localDir} brokers:${Conf.brokers}")

    kafkaDirectStream.cache

    //加载词频统计词库
    val words = BroadcastWrapper[(Long, HashSet[String])](ssc, (System.currentTimeMillis, MysqlService.getUserWords))

    //经过分词得到新的stream
    val segmentedStream = kafkaDirectStream.map(_._2).repartition(10).transform(rdd => {
      if (System.currentTimeMillis - words.value._1 > Conf.updateFreq) {
        words.update((System.currentTimeMillis, MysqlService.getUserWords), true)
        log.warn("[BroadcastWrapper] words updated")
      }
      rdd.flatMap(record => SegmentService.mapSegment(record, words.value._2))
    })

    //以entity_timestamp_beeword为key,统计本batch内各个key的计数
    val countedStream = segmentedStream.reduceByKey(_ + _)

    countedStream.foreachRDD(MysqlService.save(_))

    //更新zk中的offset
    kafkaDirectStream.foreachRDD(rdd => {
      if (!rdd.isEmpty)
        km.updateZKOffsets(rdd)
    })

    ssc
  }

  def main(args: Array[String]) {
    // Create context with 2 second batch interval
    //    val ssc = StreamingContext.getOrCreate(Conf.localDir, functionToCreateContext _)
    val ssc = functionToCreateContext()
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}