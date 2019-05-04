package sparkstreaming_action.wordfreq.service

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD

import sparkstreaming_action.wordfreq.dao.MysqlManager
import sparkstreaming_action.wordfreq.util.TimeParse
import spray.json._
import scala.collection.mutable.HashSet

/**
 * @author litaoxiao
 *
 */
object MysqlService extends Serializable {
  @transient lazy val log = LogManager.getLogger(this.getClass)

  def save(rdd: RDD[(String, Int)]) = {
    if (!rdd.isEmpty) {
      rdd.foreachPartition(partitionRecords => {
        val preTime = System.currentTimeMillis
        //从连接池中获取一个连接
        val conn = MysqlManager.getMysqlManager.getConnection
        val statement = conn.createStatement
        try {
          conn.setAutoCommit(false)
          partitionRecords.foreach(record => {
            log.info(">>>>>>>" + record)
            val createTime = System.currentTimeMillis()
            var sql = s"CREATE TABLE if not exists `word_count_${TimeParse.timeStamp2String(createTime, "yyyyMM")}`(`id` int(11) NOT NULL AUTO_INCREMENT,`word` varchar(64) NOT NULL,`count` int(11) DEFAULT '0',`date` date NOT NULL, PRIMARY KEY (`id`), UNIQUE KEY `word` (`word`,`date`) ) ENGINE=InnoDB  DEFAULT CHARSET=utf8;"
            statement.addBatch(sql)
            sql = s"insert into word_count_${TimeParse.timeStamp2String(createTime, "yyyyMM")} (word, count, date) values ('${record._1}',${record._2},'${TimeParse.timeStamp2String(createTime, "yyyy-MM-dd")}') on duplicate key update count=count+values(count);"
            statement.addBatch(sql)
            log.warn(s"[recordAddBatchSuccess] record: ${record._1}, ${record._2}")
          })
          statement.executeBatch
          conn.commit
          log.warn(s"[save_batchSaveSuccess] time elapsed: ${System.currentTimeMillis - preTime}")
        } catch {
          case e: Exception =>
            log.error("[save_batchSaveError]", e)
        } finally {
          statement.close()
          conn.close()
        }
      })
    }
  }

  /**
   * 加载管理员和用户词库
   */
  def getUserWords(): HashSet[String] = {
    val preTime = System.currentTimeMillis
    val sql = "select distinct(word) from user_words"
    val conn = MysqlManager.getMysqlManager.getConnection
    val statement = conn.createStatement
    try {
      val rs = statement.executeQuery(sql)
      val words = HashSet[String]()
      while (rs.next) {
        words += rs.getString("word")
      }
      log.warn(s"[loadSuccess] load user words from db count: ${words.size}\ttime elapsed: ${System.currentTimeMillis - preTime}")
      words
    } catch {
      case e: Exception =>
        log.error("[loadError] error: ", e)
        HashSet[String]()
    } finally {
      statement.close()
      conn.close()
    }
  }
}