package bee.test
import scala.collection.mutable.Map
import scalaj.http._

object Test {
  /**
   * 防止分词未出现，统计四元组
   * @param segStr
   * @param word
   * @param language
   * @return
   */
  def containWords(segStr: String, word: String, language: String): Boolean = {
    val segWords = segStr.split(",")
    for (i <- 0 to segWords.length) {
      for (j <- i to i + 4; if j <= segWords.length) {
        val mkWord = if (language == "zh-cn") segWords.slice(i, j).mkString("")
        else segWords.slice(i, j).mkString(" ")
        if (word == mkWord)
          return true

      }
    }
    return false
  }
  def main(args: Array[String]) {
    val map = Map[String,String]()
    map.put("你好", "zh-cn")
    map.put("你好", "zh-cn")
    map.put("你好", "zh-cn")
    map.put("你好", "zh-cn")
  }

}