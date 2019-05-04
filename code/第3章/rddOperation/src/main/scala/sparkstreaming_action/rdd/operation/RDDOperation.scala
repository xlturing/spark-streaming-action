package sparkstreaming_action.rdd.operation

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object RDDOperation extends App {
  val conf = new SparkConf()
    .setAppName("rddOperation")
    .setMaster("spark://127.0.0.1:7077")
  // Create spark context
  val sc = new SparkContext(conf)
  val txtNameAddr = "name_addr.txt"
  val txtNamePhone = "name_phone.txt"
  val rddNameAddr = sc.textFile(txtNameAddr).map(record => {
    val tokens = record.split(" ")
    (tokens(0), tokens(1))
  }) // rdd1
  rddNameAddr.cache
  val rddNamePhone = sc.textFile(txtNamePhone).map(record => {
    val tokens = record.split(" ")
    (tokens(0), tokens(1))
  }) // rdd2
  rddNamePhone.cache
  val rddNameAddrPhone = rddNameAddr.join(rddNamePhone) // rdd3
  val rddHtml = rddNameAddrPhone.map(record => {
    val name = record._1
    val addr = record._2._1
    val phone = record._2._2
    s"<h2>姓名：${name}</h2><p>地址：${addr}</p><p>电话：${phone}</p>"
  }) // rdd4
  val rddOutput = rddHtml.saveAsTextFile("UserInfo")

  val rddPostcode = rddNameAddr.map(record => {
    val postcode = record._2.split("#")(1)
    (postcode, 1)
  }) // rdd5
  val rddPostcodeCount = rddPostcode.reduceByKey(_ + _) // rdd6
  rddPostcodeCount.collect().foreach(println)
  sc.stop
}