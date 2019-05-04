package sparkstreaming_action.alarm.service

import scala.collection.mutable.MutableList
import scala.collection.mutable.Map

import org.apache.log4j.LogManager

import scalaj.http._
import spray.json._
import spray.json.DefaultJsonProtocol._

import sparkstreaming_action.alarm.util._
import sparkstreaming_action.alarm.entity.MonitorGame

import com.huaban.analysis.jieba.JiebaSegmenter
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import scala.collection.JavaConversions

object SegmentService extends Serializable {
  @transient lazy val log = LogManager.getLogger(this.getClass)

  /**
   * 将文本内容分词
   * @param json
   * @return
   */
  def mapSegment(json: String, monitorGames: Map[Int, MonitorGame]): Option[(Int, String)] = {
    val preTime = System.currentTimeMillis
    try {
      json.parseJson.asJsObject.getFields("gameId", "review") match {
        case Seq(JsString(gameId), JsString(review)) => {
          if (!monitorGames.contains(gameId.toInt)) {
            log.warn(s"[ignored] no need to monitor gameId: ${gameId}");
            None
          } else {
            try {
              if (review.trim() == "") {
                log.warn(s"[reviewEmptyError] json: ${json}")
                None
              } else {
                val ge = monitorGames.get(gameId.toInt).get
                // 返回json结果  
                val jo = JsObject(
                  "gameId" -> JsNumber(ge.gameId),
                  "review" -> JsString(review),
                  "reviewSeg" -> JsString(segment(filter(review))),
                  "gameName" -> JsString(ge.gameName))
                log.warn(s"[Segment Success] gameId: ${ge.gameId}\tgameName: ${ge.gameName}\t" +
                  s"time elapsed: ${System.currentTimeMillis - preTime}\t" +
                  s"MonitorGame count: ${monitorGames.size}")
                Some((ge.gameId, jo.toString))
              }
            } catch {
              case e: Exception => {
                log.error(s"[Segment Error] mapSegment error\tjson string: ${json}\treview: ${review}", e)
                None
              }
            }
          }
        }
        case _ => {
          log.warn(s"[Segment Match Failed] json parse match failed! error json is:\n${json}")
          None
        }
      }
    } catch {
      case e: Exception => {
        log.error(s"[Segment Json Parse Error] mapSegment error\tjson string: ${json}", e)
        None
      }
    }
  }

  def filter(s: String): String = {
    return s.replace("\t", " ");
  }

  def segment(review: String): String = {
    val seg = new JiebaSegmenter
    var ts = seg.process(review, SegMode.SEARCH);
    val words = MutableList[String]()
    for (t <- JavaConversions.asScalaBuffer(ts)) {
      words += t.word
    }
    words.mkString("\t")
  }
}