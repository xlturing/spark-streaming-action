package sparkstreaming_action.wordfreq.service

import scala.collection.mutable.HashSet
import scala.collection.mutable.Map

import org.apache.log4j.LogManager

import sparkstreaming_action.wordfreq.util.Conf
import scalaj.http._
import spray.json._
import spray.json.DefaultJsonProtocol._

object SegmentService extends Serializable {
  @transient lazy val log = LogManager.getLogger(this.getClass)

  /**
   * 将文本内容分词
   * @param json
   * @return
   */
  def mapSegment(record: String, wordDic: HashSet[String]): Map[String, Int] = {
    val preTime = System.currentTimeMillis
    val keyCount = Map[String, Int]()
    if (record == "" || record.isEmpty()) {
      log.warn(s"record is empty: ${record}")
      keyCount
    } else {
      val postUrl = Conf.segmentorHost + "/token/"
      try {
        val wordsSet = retry(3)(segment(postUrl, record))
        log.warn(s"[mapSegmentSuccess] record: ${record}\ttime elapsed: ${System.currentTimeMillis - preTime}")
        // 进行词语统计
        val keyCount = Map[String, Int]()
        for (word <- wordDic) {
          if (wordsSet.contains(word))
            keyCount += word -> 1
        }
        log.warn(s"[keyCountSuccess] words size: ${wordDic.size} (entitId_createTime_word_language, 1):\n${keyCount.mkString("\n")}")
        keyCount
      } catch {
        case e: Exception => {
          log.error(s"[mapSegmentApiError] mapSegment error\tpostUrl: ${postUrl}${record}", e)
          keyCount
        }
      }
    }
  }

  def segment(url: String, content: String): HashSet[String] = {
    val timer = System.currentTimeMillis()
    var response = Http(url + content).asString
    val dur = System.currentTimeMillis() - timer
    if (dur > 20) // 输出耗时较长的请求
      log.warn(s"[longVisit]>>>>>> api: ${url}${content}\ttimer: ${dur}")
    val words = HashSet[String]()
    response.code match {
      case 200 => {
        response.body.parseJson.asJsObject.getFields("ret", "msg", "terms") match {
          case Seq(JsNumber(ret), JsString(msg), JsString(terms)) => {
            if (ret.toInt != 0) {
              log.error(s"[segmentRetError] vist api: ${url}?content=${content}\tsegment error: ${msg}")
              words
            } else {
              val tokens = terms.split(" ")
              tokens.foreach(token => {
                words += token
              })
              words
            }
          }
          case _ => words
        }
      }
      case _ => {
        log.error(s"[segmentResponseError] vist api: ${url}?content=${content}\tresponse code: ${response.code}")
        words
      }
    }
  }

  /**
   * retry common function
   * @param n
   * @param fn
   * @return
   */
  @annotation.tailrec
  def retry[T](n: Int)(fn: => T): T = {
    util.Try { fn } match {
      case util.Success(x) => x
      case _ if n > 1 => {
        log.warn(s"[retry ${n}]")
        retry(n - 1)(fn)
      }
      case util.Failure(e) => {
        log.error(s"[segError] API retry 3 times fail!!!!! T^T召唤神龙吧！", e)
        throw e
      }
    }
  }
}