package sparkstreaming_action.save2file.main

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object Save2File {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Save2File_SparkStreaming")
      .setMaster("spark://127.0.0.1:7077")
    // Create spark streaming context
    val ssc = new StreamingContext(conf, Seconds(3))
    val input = args(0) + "/input"
    val output = args(0) + "/output"
    println("read file name: " + input + "\nout file name: " + output)
    // 从磁盘上读取文本文件作为输入流
    val textStream = ssc.textFileStream(input)
    // 进行词频统计
    val wcStream = textStream.flatMap { line => line.split(" ") }
      .map { word => (word, 1) }
      .reduceByKey(_ + _)
    // 打印到控制台并保存为文本文件和序列化文件
    wcStream.print()
    wcStream.saveAsTextFiles("file://" + output + "/saveAsTextFiles")
    wcStream.saveAsObjectFiles("file://" + output + "/saveAsObjectFiles")

    ssc.start()
    ssc.awaitTermination()
  }
}