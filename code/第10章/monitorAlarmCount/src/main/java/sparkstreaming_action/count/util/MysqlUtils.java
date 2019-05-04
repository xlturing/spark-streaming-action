package sparkstreaming_action.count.util;

import java.beans.PropertyVetoException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ArrayHandler;
import org.apache.commons.dbutils.handlers.ArrayListHandler;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import sparkstreaming_action.count.entity.Alarm;

/**
 * 该类利用common-DBUtils提供数据库查询服务
 * 
 * @author litaoxiao
 * 
 */
public class MysqlUtils {
	private static Logger log = Logger.getLogger(MysqlUtils.class);
	private static Connection conn = null;
	private static ComboPooledDataSource cpds = new ComboPooledDataSource();

	static {
		try {
			cpds.setDriverClass("com.mysql.jdbc.Driver");
			cpds.setJdbcUrl(ConfigUtils.getConfig("url"));
			cpds.setUser(ConfigUtils.getConfig("username"));
			cpds.setPassword(ConfigUtils.getConfig("password"));
			cpds.setMinPoolSize(5);
			cpds.setAcquireIncrement(5);
			cpds.setMaxPoolSize(20);
			cpds.setMaxStatements(180);
			// 检测连接配置
			cpds.setPreferredTestQuery("SELECT 1");
			cpds.setIdleConnectionTestPeriod(10000);
			// 获取到连接时就同步检测
			cpds.setTestConnectionOnCheckin(true);
		} catch (PropertyVetoException e) {
			log.error(ExceptionUtils.getStackTrace(e));
		}
	}

	public static Connection getConnection() {
		try {
			conn = cpds.getConnection();
		} catch (Exception e) {
			log.error(ExceptionUtils.getStackTrace(e));
		}
		return conn;
	}

	/**
	 * 获取指定db table的更新时间
	 * 
	 * @param db
	 * @param table
	 * @return
	 */
	public static long getUpdateTime(String table) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			return sdf.parse(MysqlUtils.queryByMapHandler(String.format(
					"select TABLE_NAME,UPDATE_TIME from information_schema.TABLES where information_schema.TABLES.TABLE_NAME = '%s';",
					table)).get("UPDATE_TIME").toString()).getTime() / 1000;
		} catch (Exception e) {
			log.error(ExceptionUtils.getStackTrace(e));
			return 0L;
		}
	}

	/**
	 * 指定表名和对象，进行插入操作
	 * 
	 * @param table
	 * @param o
	 */
	public static void insert(String table, Object o) {
		try {
			QueryRunner runner = new QueryRunner();
			String sql = String.format("insert into %s (%s) values(%s);", table, StringUtils.join(getFiledName(o), ","),
					StringUtils.join(getFiledValues(o), ","));
			runner.update(getConnection(), sql);
		} catch (Exception e) {
			log.error(ExceptionUtils.getStackTrace(e));
		} finally {
			destroy();
		}
	}

	/**
	 * 会返回一个集合，集合中的每一项对应结果集指定行中的数据转换后的数组。
	 * 
	 * @param sql
	 * @return
	 */
	public static List<Object[]> queryByArrayListHandler(String sql) {
		List<Object[]> rs = null;
		try {
			QueryRunner runner = new QueryRunner();
			rs = runner.query(getConnection(), sql, new ArrayListHandler());
		} catch (Exception e) {
			log.error(ExceptionUtils.getStackTrace(e));
		} finally {
			destroy();
		}

		return rs;
	}

	/**
	 * 返回一个数组，用于将结果集第一行数据转换为数组
	 * 
	 * @param sql
	 * @return
	 */
	public static Object[] queryByArrayHandler(String sql) {
		Object[] rs = null;
		try {
			QueryRunner runner = new QueryRunner();
			rs = runner.query(getConnection(), sql, new ArrayHandler());
		} catch (Exception e) {
			log.error(ExceptionUtils.getStackTrace(e));
		} finally {
			destroy();
		}

		return rs;
	}

	/**
	 * 将sql查询的结果集中的第一行转换为键值对，键为列名
	 * 
	 * @param sql
	 * @return
	 */
	public static Map<String, Object> queryByMapHandler(String sql) {
		Map<String, Object> rs = null;
		try {
			QueryRunner runner = new QueryRunner();
			rs = runner.query(getConnection(), sql, new MapHandler());
		} catch (Exception e) {
			log.error(ExceptionUtils.getStackTrace(e));
		} finally {
			destroy();
		}

		return rs;
	}

	/**
	 * 将sql查询的结果集转换为一个集合，集合中的数据为对应行转换的键值对，键为列名
	 * 
	 * @param sql
	 * @return
	 */
	public static List<Map<String, Object>> queryByMapListHandler(String sql) {
		List<Map<String, Object>> rs = null;
		try {
			QueryRunner runner = new QueryRunner();
			rs = runner.query(getConnection(), sql, new MapListHandler());
		} catch (Exception e) {
			log.error(ExceptionUtils.getStackTrace(e));
		} finally {
			destroy();
		}

		return rs;
	}

	/**
	 * 泛型函数，将结果集中的第一行转换为指定Bean
	 * 
	 * @param sql
	 * @param beanType
	 * @return
	 */
	public static <T> T queryByBeanHandler(String sql, Class<T> beanType) {
		T rs = null;
		try {
			QueryRunner runner = new QueryRunner();
			rs = runner.query(getConnection(), sql, new BeanHandler<T>(beanType));
		} catch (Exception e) {
			log.error(ExceptionUtils.getStackTrace(e));
		} finally {
			destroy();
		}
		return rs;
	}

	/**
	 * 泛型函数，将结果集中所有行转换为指定Bean的List
	 * 
	 * @param sql
	 * @param beanType
	 * @return
	 */
	public static <T> List<T> queryByBeanListHandler(String sql, Class<T> beanType) {
		List<T> rs = null;
		try {
			QueryRunner runner = new QueryRunner();
			rs = runner.query(getConnection(), sql, new BeanListHandler<T>(beanType));
		} catch (Exception e) {
			log.error(ExceptionUtils.getStackTrace(e));
		} finally {
			destroy();
		}
		return rs;
	}

	public static void destroy() {
		DbUtils.closeQuietly(conn);
	}

	/**
	 * 获取对象的所有属性值，返回一个list
	 * 
	 * @param o
	 * @return
	 */
	private static List<String> getFiledValues(Object o) {
		Field[] fields = o.getClass().getDeclaredFields();
		List<String> list = new ArrayList<String>();
		for (int i = 0; i < fields.length; i++) {
			if (fields[i].getName().equals("id"))
				continue;// id 一般为自增
			String value = getFieldValueByName(fields[i].getName(), o);
			String type = fields[i].getType().toString();
			if (type.equals("class java.lang.String"))
				value = "'" + value + "'";
			list.add(value);
		}
		return list;
	}

	/**
	 * 获取属性名数组
	 * 
	 * @param o
	 * @return
	 */
	private static List<String> getFiledName(Object o) {
		Field[] fields = o.getClass().getDeclaredFields();
		List<String> fieldNames = new ArrayList<String>();
		for (int i = 0; i < fields.length; i++) {
			if (fields[i].getName().equals("id"))
				continue;// id 一般为自增
			fieldNames.add(fields[i].getName());
		}
		return fieldNames;
	}

	/**
	 * 获取对象的所有属性值，返回一个对象数组
	 * 
	 * @param fieldName
	 * @param o
	 * @return
	 */
	private static String getFieldValueByName(String fieldName, Object o) {
		try {
			String firstLetter = fieldName.substring(0, 1).toUpperCase();
			String getter = "get" + firstLetter + fieldName.substring(1);
			Method method = o.getClass().getMethod(getter, new Class[] {});
			Object value = method.invoke(o, new Object[] {});
			return value.toString();
		} catch (Exception e) {
			return null;
		}
	}

	public static void main(String[] args) {
		Alarm alarm = new Alarm();
		alarm.game_id = 2;
		alarm.game_name = "haha";
		insert("internal_risk_warn_items", alarm);
		System.out.println(StringUtils.join(getFiledValues(new Alarm()), ","));
	}
}
