package sparkstreaming_action.producer.main
import java.util.Properties
import scala.util.Random
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

object Producer extends App {
  val events = args(0).toInt
  val topic = args(1)
  val brokers = args(2)
  val rnd = new Random()
  val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("client.id", "wordFreqGenerator")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)
  val t = System.currentTimeMillis()

  // 读取汉字字典
  val source = scala.io.Source.fromFile("hanzi.txt")
  val lines = try source.mkString finally source.close()
  for (nEvents <- Range(0, events)) {
    // 生成模拟评论数据(user, comment)
    val sb = new StringBuilder()
    for (ind <- Range(0, rnd.nextInt(200))) {
      sb += lines.charAt(rnd.nextInt(lines.length()))
    }
    val userName = "user_" + rnd.nextInt(100)
    val data = new ProducerRecord[String, String](topic, userName, sb.toString())

    //async
    //producer.send(data, (m,e) => {})
    //sync
    producer.send(data)
  }

  System.out.println("sent per second: " + events * 1000 / (System.currentTimeMillis() - t))
  producer.close()
}