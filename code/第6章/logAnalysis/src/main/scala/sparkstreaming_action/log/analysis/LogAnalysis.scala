package sparkstreaming_action.log.analysis

import java.util.Properties
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

case class Record(log_level: String, method: String, content: String)
object LogAnalysis extends App {
  val sparkConf = new SparkConf().setAppName("LogAnalysis").setMaster("spark://localhost:7077")
    .set("spark.local.dir", "./tmp")
  val spark = SparkSession.builder()
    .appName("LogAnalysis")
    .config(sparkConf)
    .getOrCreate()
  val sc = spark.sparkContext
  val ssc = new StreamingContext(sc, Seconds(2))

  // Mysql配置
  val properties = new Properties()
  properties.setProperty("user", "root")
  properties.setProperty("password", "root")

  // 读入日志文件目录下的日志信息流
  val logStream = ssc.textFileStream("./logs")
  // 将日志信息流转换为dataframe
  logStream.foreachRDD((rdd: RDD[String]) => {
    import spark.implicits._
    val data = rdd.map(w => {
      val tokens = w.split("\t")
      Record(tokens(0), tokens(1), tokens(2))
    }).toDF()
    data.createOrReplaceTempView("alldata")

    // 条件筛选
    val logImp = spark.sql("select * from alldata where log_level='[error]' or log_level='[warn]'")
    logImp.show()
    // 输出到外部Mysql中
    val schema = StructType(Array(StructField("log_level", StringType, true)
        , StructField("method", StringType, true)
        , StructField("content", StringType, true)))
    logImp.write.mode(SaveMode.Append)
      .jdbc("jdbc:mysql://localhost:3306/log_analysis", "important_logs", properties)
  })
  ssc.start()
  ssc.awaitTermination()
}