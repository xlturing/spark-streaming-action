package sparkstreaming_action.alarm.dao

import java.sql.Connection
import com.mchange.v2.c3p0.ComboPooledDataSource
import sparkstreaming_action.alarm.util.Conf
import org.apache.log4j.LogManager

/**
 * Mysql连接池类(c3p0)
 * @author litaoxiao
 *
 */
class MysqlPool extends Serializable {
  @transient lazy val log = LogManager.getLogger(this.getClass)

  private val cpds: ComboPooledDataSource = new ComboPooledDataSource(true)
  private val conf = Conf.mysqlConfig
  try {
    cpds.setJdbcUrl(conf.get("url").getOrElse("jdbc:mysql://10.217.32.142:3306/test_bee?useUnicode=true&amp;characterEncoding=UTF-8"));
    cpds.setDriverClass("com.mysql.jdbc.Driver");
    cpds.setUser(conf.get("username").getOrElse("root"));
    cpds.setPassword(conf.get("password").getOrElse(""))
    cpds.setInitialPoolSize(3)
    cpds.setMaxPoolSize(Conf.maxPoolSize)
    cpds.setMinPoolSize(Conf.minPoolSize)
    cpds.setAcquireIncrement(5)
    cpds.setMaxStatements(180)
    <!--最大空闲时间,25000秒内未使用则连接被丢弃。若为0则永不丢弃。Default: 0 -->
    cpds.setMaxIdleTime(25000)
    // 检测连接配置
    cpds.setPreferredTestQuery("select id from word_count_201610 where id = 1")
    cpds.setIdleConnectionTestPeriod(18000)
  } catch {
    case e: Exception =>
      log.error("[MysqlPoolError]", e)
  }
  def getConnection: Connection = {
    try {
      return cpds.getConnection();
    } catch {
      case e: Exception =>
        log.error("[MysqlPoolGetConnectionError]", e)
        null
    }
  }
}
object MysqlManager {
  var mysqlManager: MysqlPool = _
  def getMysqlManager: MysqlPool = {
    synchronized {
      if (mysqlManager == null) {
        mysqlManager = new MysqlPool
      }
    }
    mysqlManager
  }
}