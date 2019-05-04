package sparkstreaming_action.userbehavior.util

import redis.clients.jedis.exceptions.JedisConnectionException
import redis.clients.jedis.Jedis
import scala.collection.mutable.ArrayBuffer

object RedisUtil {
  var readJedis: Jedis = _
  var writeJedis: Jedis = _

  def checkAlive {
    if (!isConnected(readJedis))
      readJedis = reConect(readJedis)
    if (!isConnected(writeJedis))
      writeJedis = reConect(writeJedis)
  }

  def getConn(ip: String, port: Int, passwd: String) = {
    val jedis = new Jedis(ip, port, 5000)
    jedis.connect
    if (passwd.length > 0)
      jedis.auth(passwd)
    jedis
  }

  def isConnected(jedis: Jedis) = jedis != null && jedis.isConnected

  def reConect(jedis: Jedis) = {
    println("reconnecting ...")
    disConnect(jedis)
    getConn(Conf.redisIp, Conf.redisPort, Conf.passwd)
  }

  def disConnect(jedis: Jedis) {
    if (jedis != null && jedis.isConnected()) {
      jedis.close
    }
  }

  def batchSet(kvs: Seq[(Array[Byte], Array[Byte])]) {
    try {
      var i = 0
      while (i < kvs.length) {
        val pipeline = writeJedis.pipelined
        val target = i + Conf.batchSize
        //println(s"set ${new String(kvs(0)._1)} to ${kvs(0)._2}")
        while (i < target && i < kvs.length) {
          pipeline.set(kvs(i)._1, kvs(i)._2)
          pipeline.expire(kvs(i)._1, Conf.EXPIRE_DURATION)
          i += 1
        }
        pipeline.sync
      }
    } catch {
      case connEx: JedisConnectionException => reConect(writeJedis)
      case ex: Exception                    => ex.printStackTrace
    } finally {

    }
  }

  def batchGet(keys: Seq[String]) = {
    val res = ArrayBuffer[Array[Byte]]()
    try {
      checkAlive
      var i = 0
      while (i < keys.length) {
        val target = i + Conf.batchSize
        while (i < target && i < keys.length) {
          val resp = readJedis.get(keys(i).getBytes)
          res += resp
          i += 1
        }
      }
      res
    } catch {
      case connEx: JedisConnectionException => reConect(readJedis)
      case ex: Exception                    => ex.printStackTrace
    }
    res
  }
}