package sparkstreaming_action.crawler.main
import java.util.Properties

import scala.util.Random

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

import spray.json._
import DefaultJsonProtocol._

object Producer extends App {
  val pageNumPerGame = args(0).toInt
  val topic = args(1)
  val brokers = args(2)
  val rnd = new Random()
  val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("client.id", "monitorAlarmCrawler")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)
  val t = System.currentTimeMillis()

  // 从taptap上爬取用户评论数据(game_id, comments)
  val crawlerData = Crawler.crawData(pageNumPerGame)

  var events = 0
  for (e <- crawlerData) {
    val (game_id, reviews) = e
    reviews.foreach(review => {
      val revUtf8 = new String(review.getBytes, 0, review.length, "UTF8")
      val data = new ProducerRecord[String, String](topic, game_id.toString, Map("gameId" -> game_id.toString, "review" -> revUtf8).toJson.toString)
      producer.send(data)
      events += 1
    })
  }

  System.out.println("sent per second: " + events * 1000 / (System.currentTimeMillis() - t))
  producer.close()
}