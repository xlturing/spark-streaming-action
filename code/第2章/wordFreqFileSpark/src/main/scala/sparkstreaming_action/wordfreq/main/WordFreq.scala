package sparkstreaming_action.wordfreq.main

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordFreq {
  def main(args: Array[String]) {
    // Create spark context
    val conf = new SparkConf()
      .setAppName("WordFreq_Spark")
      .setMaster("spark://127.0.0.1:7077")
    val sc = new SparkContext(conf)
    val txtFile = "input.txt"
    val txtData = sc.textFile(txtFile)
    txtData.cache()
    txtData.count()
    val wcData = txtData.flatMap { line => line.split(" ") }
      .map { word => (word, 1) }
      .reduceByKey(_ + _)
    wcData.collect().foreach(println)

    sc.stop
  }
}