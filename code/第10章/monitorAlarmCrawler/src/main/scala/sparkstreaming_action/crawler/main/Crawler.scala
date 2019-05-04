package sparkstreaming_action.crawler.main

import scala.collection.mutable.Map
import sparkstreaming_action.crawler.dao.MysqlManager
import org.apache.log4j.LogManager
import kantan.xpath._
import kantan.xpath.implicits._
import kantan.xpath.nekohtml._
import scala.io.Source
import scala.collection.mutable.ListBuffer

object Crawler {
  @transient lazy val log = LogManager.getLogger(this.getClass)

  /**
   * 加载游戏库
   */
  def getGames: Map[Int, String] = {
    val preTime = System.currentTimeMillis
    //目前只关注中文渠道language=zh-cn
    var sql = "select * from games"
    val conn = MysqlManager.getMysqlManager.getConnection
    val statement = conn.createStatement
    try {
      val rs = statement.executeQuery(sql)
      val games = Map[Int, String]()
      while (rs.next) {
        games += (rs.getInt("game_id") -> rs.getString("game_name"))
      }
      log.warn(s"[loadSuccess] load entities from db count: ${games.size}\ttime elapsed: ${System.currentTimeMillis - preTime}")
      games
    } catch {
      case e: Exception =>
        log.error("[loadError] error: ", e)
        Map[Int, String]()
    } finally {
      statement.close()
      conn.close()
    }
  }

  /**
   * 从taptap上爬取用户评论数据
   */
  def crawData(pageNum: Int): Map[Int, List[String]] = {
    val games = getGames
    println(games)
    val data = Map[Int, List[String]]()
    games.foreach(e => {
      val (game_id, game_name) = e
      val reviews = ListBuffer[String]()
      for (page <- 1 until pageNum + 1) { // 20 reviews per page
        val url = s"https://www.taptap.com/app/$game_id/review?order=default&page=$page#review-list"
        println(url)
        val html = Source.fromURL(url).mkString
        val rs = html.evalXPath[List[String]](xp"//div[@class='item-text-body']/p")
        if (rs.isRight)
          reviews ++= rs.right.get
      }
      log.info(s"$game_name craw data size: ${reviews.size}")
      data += (game_id -> reviews.toList)
    })
    log.info(s"craw all data done, size: ${data.values.size}\nfirst is ${data(2301)(0)}")
    data
  }

  def main(args: Array[String]) {
    crawData(1)
  }
}