package sparkstreaming_action.count.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 时间类的通用函数
 * 
 * @author litaoxiao
 * 
 */
public class TimeUtils {
	/**
	 * 将指定时间格式化yyyy-MM-dd HH:mm:ss
	 * 
	 * @param timeSec
	 * @return
	 */
	public static String timeToString(Long timeSec) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sdf.format(new Date(timeSec * 1000));
	}

	/**
	 * 获取指定时间戳(s)的开始时间00:00:00
	 * 
	 * @param timeSec
	 * @return
	 */
	public static Long getStartOfTime(Long timeSec) {
		Calendar cur = Calendar.getInstance();
		cur.setTimeInMillis(timeSec * 1000);
		cur.set(Calendar.HOUR_OF_DAY, 0);
		cur.set(Calendar.MINUTE, 0);
		cur.set(Calendar.SECOND, 0);
		cur.set(Calendar.MILLISECOND, 0);
		return cur.getTimeInMillis() / 1000;
	}

	/**
	 * 获取指定时间戳(s)的结束时间第二天的00:00:00
	 * 
	 * @param timeSec
	 * @return
	 */
	public static Long getEndOfTime(Long timeSec) {
		return getStartOfTime(timeSec) + 86400;
	}

	/**
	 * 获取当前时间戳(s)
	 * 
	 * @return
	 */
	public static long currentTimeSeconds() {
		return System.currentTimeMillis() / 1000;
	}

	public static void main(String[] args) {
		System.out.println(getEndOfTime(currentTimeSeconds()) - 86400);
	}
}
