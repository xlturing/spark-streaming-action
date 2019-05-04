package sparkstreaming_action.alarm.service

import spray.json._
import sparkstreaming_action.alarm.entity.Record

object MyJsonProtocol extends DefaultJsonProtocol {
  implicit val docFormat = jsonFormat2(Record)
}

/**
 * 类与json字符串的互相转换
 * @author litaoxiao
 */
object JsonParse {
  import spray.json._
  import MyJsonProtocol._

  def record2Json(doc: Record): String = {
    doc.toJson.toString()
  }

  def json2Record(json: String): Record = {
    json.parseJson.convertTo[Record]
  }

}