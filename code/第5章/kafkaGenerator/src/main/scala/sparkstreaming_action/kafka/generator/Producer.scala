package sparkstreaming_action.kafka.generator
import java.util.Properties
import scala.util.Random
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

object Producer extends App {
  val topic = args(0)
  val brokers = args(1)
  val rnd = new Random()
  val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("client.id", "kafkaGenerator")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)
  val t = System.currentTimeMillis()
  val nameAddrs = Map("bob" -> "shanghai#200000", "amy" -> "beijing#100000", "alice" -> "shanghai#200000",
    "tom" -> "beijing#100000", "lulu" -> "hangzhou#310000", "nick" -> "shanghai#200000")
  val namePhones = Map("bob" -> "15700079421", "amy" -> "18700079458", "alice" -> "17730079427",
    "tom" -> "16700379451", "lulu" -> "18800074423", "nick" -> "14400033426")
  // 生成模拟数据(name, addr, type:0)
  for (nameAddr <- nameAddrs) {
    val data = new ProducerRecord[String, String](topic, nameAddr._1, s"${nameAddr._1}\t${nameAddr._2}\t0")
    producer.send(data)
    if (rnd.nextInt(100) < 50) Thread.sleep(rnd.nextInt(10))
  }
  // 生成模拟数据(name, addr, type:1)
  for (namePhone <- namePhones) {
    val data = new ProducerRecord[String, String](topic, namePhone._1, s"${namePhone._1}\t${namePhone._2}\t1")
    producer.send(data)
    if (rnd.nextInt(100) < 50) Thread.sleep(rnd.nextInt(10))
  }

  System.out.println("sent per second: " + (nameAddrs.size + namePhones.size) * 1000 / (System.currentTimeMillis() - t))
  producer.close()
}