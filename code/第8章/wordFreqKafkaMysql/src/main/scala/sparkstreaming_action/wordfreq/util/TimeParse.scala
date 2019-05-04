package sparkstreaming_action.wordfreq.util

import org.joda.time.{ DateTimeZone, _ }
import org.joda.time.format.DateTimeFormat

object TimeParse extends Serializable {
  def timeStamp2String(timeStamp: String, format: String): String = {
    val ts = timeStamp.toLong * 1000;
    new DateTime(ts).toDateTime.toString(format)
  }

  def timeStamp2String(timeStamp: Long, format: String): String = {
    new DateTime(timeStamp).toDateTime.toString(format)
  }
}