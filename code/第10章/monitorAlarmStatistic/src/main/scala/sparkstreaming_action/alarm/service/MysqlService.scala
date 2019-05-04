package sparkstreaming_action.alarm.service

import scala.collection.mutable.Map

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD

import spray.json._
import sparkstreaming_action.alarm.dao.MysqlManager
import sparkstreaming_action.alarm.entity.MonitorGame

/**
 * @author litaoxiao
 *
 */
object MysqlService extends Serializable {
  @transient lazy val log = LogManager.getLogger(this.getClass)
  /**
   * 加载监控游戏库
   */
  def getGames(): Map[Int, MonitorGame] = {
    val preTime = System.currentTimeMillis
    //目前只关注中文渠道language=zh-cn
    var sql = "select * from monitor_games"
    val conn = MysqlManager.getMysqlManager.getConnection
    val statement = conn.createStatement
    try {
      val rs = statement.executeQuery(sql)
      val games = Map[Int, MonitorGame]()
      while (rs.next) {
        games += (rs.getInt("game_id") -> new MonitorGame(
          rs.getInt("game_id"),
          rs.getString("game_name")))
      }
      log.warn(s"[loadSuccess] load entities from db count: ${games.size}\ttime elapsed: ${System.currentTimeMillis - preTime}")
      games
    } catch {
      case e: Exception =>
        log.error("[loadError] error: ", e)
        Map[Int, MonitorGame]()
    } finally {
      statement.close()
      conn.close()
    }
  }
}