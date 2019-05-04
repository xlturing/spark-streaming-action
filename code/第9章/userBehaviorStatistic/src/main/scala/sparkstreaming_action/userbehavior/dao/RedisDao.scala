package sparkstreaming_action.userbehavior.dao
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisConnectionException
import redis.clients.jedis.Response
import scala.collection.mutable.ArrayBuffer
import java.io.BufferedReader
import java.io.InputStreamReader

import sparkstreaming_action.userbehavior.util.Conf
import sparkstreaming_action.userbehavior.util.Tool
import sparkstreaming_action.userbehavior.util.RedisUtil

object RedisDao {
  def constructKey(uid: String) = uid + "_freq"

  def updateRedis(_records: Seq[Conf.DT]) {
    val start = System.currentTimeMillis()
    val records = _records.map {
      case (uid, updates) =>
        (constructKey(uid), updates)
    }
    val uids = records.map(_._1)
    val rawVals = RedisUtil.batchGet(uids)
    val recordAndVal = records.zip(rawVals)
    val set = new ArrayBuffer[(Array[Byte], Array[Byte])]
    for (((key, updates), rawVal) <- recordAndVal) {
      updateRedis(key, updates, rawVal, set)
    }
    RedisUtil.batchSet(set)
    if (math.random < 0.1)
      println(s"deal with ${records.length} records in ${System.currentTimeMillis() - start} ms.")
  }

  def updateRedis(key: String, updates: ArrayBuffer[(Long, Long)], rawVal: Array[Byte],
                  set: ArrayBuffer[(Array[Byte], Array[Byte])]) {
    var table = Array[Array[Long]]()
    if (rawVal != null && rawVal.length > 0) {
      val record = rawVal
      //      try {
      //restore array from byte array
      table = Tool.decodeArray(record)
      table = trimTable(table)
      //      } catch {
      //        case ex: Exception => throw new RuntimeException(s"rawVal=${rawVal}, ex=${ex.getMessage}")
      //      }
    }

    val newTable = updateRecord(table, updates, key)
    if (newTable.length != 0) {
      //encode array to byte array
      val newValue = Tool.encodeArray(newTable)
      set += ((key.getBytes, newValue))
    }
  }

  def updateRecord(_table: Array[Array[Long]], _vals: ArrayBuffer[(Long, Long)], key: String) = {

    val table = _table.map { x => (x(0), x(1)) }
    // print some log for observation
    if (table.length > 0 && math.random < 0.1) {
      println(s"query $key and get ${table.mkString("\t")}")
    }
    val vals = _vals.map { x => (x._1, x._2) }
    val union = (table ++ vals).sorted

    val newTable = ArrayBuffer[Array[Long]]()
    var preItem = -1L
    var cnt = 0

    var i = union.length - 1
    while (i >= 0) {
      val (item, timestamp) = union(i)
      //println(s"isInvalidate(${timestamp}, ${windowSize}) = ${isInvalidate(timestamp, windowSize)}")
      if (!Tool.isInvalidate(timestamp, Conf.windowSize)) {
        if (item == preItem) {
          cnt += 1
        } else {
          preItem = item
          cnt = 1
        }

        if (cnt <= Conf.MAX_CNT) {
          newTable += Array(item, timestamp)
        }
      }
      i -= 1
    }

    newTable.reverse.toArray
  }

  def trimTable(table: Array[Array[Long]]) = {
    val buf = new ArrayBuffer[Array[Long]]
    for (i <- 0 until table.length) {
      if (table(i).length == Conf.RECORD_SZ && table(i)(Conf.INDEX_TIEMSTAMP) > 0)
        buf += table(i)
      else {
        println("discard " + table(i).mkString(","))
      }
    }
    buf.toArray
  }
}