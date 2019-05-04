package sparkstreaming_action.userbehavior.main

import scala.collection.mutable.ArrayBuffer
import sparkstreaming_action.userbehavior.util.Conf

/**
 * 利用mapWithState时需要依赖的State设计
 */
class FrequencyStrategy extends RealStrategy {
  val MAX = 25

  override def getKeyFields = Array(Conf.INDEX_LOG_USER, Conf.INDEX_LOG_ITEM)

  override def update(log: Seq[Array[String]], previous: ArrayBuffer[Long]) = {
    var logTime = log.map(_(Conf.INDEX_LOG_TIME).toLong)
    if (logTime.length > MAX) {
      println("exceed max length:\n" + log.map { _.mkString("\t") }.mkString("\n"))
    }
    logTime = logTime.slice(logTime.length - MAX, logTime.length)
    val x = previous
    var status = trim(x)

    if (math.random < 0.00005)
      println(s"[processed]: qq:${log(0)(Conf.INDEX_LOG_ITEM)}, order:${log(0)(Conf.INDEX_LOG_USER)}${logTime(0)}" +
        s" at ${System.currentTimeMillis() / 1000}, x=${status.mkString(",")}")

    val shouldRemove = logTime.length + status.length - MAX
    if (shouldRemove > 0)
      for (i <- 0 until shouldRemove if status.length > 0)
        status.remove(0)

    status ++= logTime

    status
  }

  def remove(arr: ArrayBuffer[Long], ele: Long) = {
    while (arr.length > 0 && arr(0) <= ele)
      arr.remove(0)
    arr
  }
}