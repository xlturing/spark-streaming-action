package sparkstreaming_action.count.util;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;

/**
 * 一些公用工具函数
 * 
 * @author litaoxiao
 * 
 */
/**
 * @author litaoxiao
 * 
 */
public class CommonUtils {
	private static long lastFileUpdateTime = 0L; // 多个文件上次更新时间

	/**
	 * 传入多个文件名，任何一个文件有变动则返回true (lastFileUpdateTime)
	 * 
	 * @param files
	 * @return
	 */
	public static boolean isFileChange(String... files) {
		boolean flag = false;
		for (String file : files) {
			URL res = CommonUtils.class.getResource("/" + file);
			if (res == null)
				continue;

			long updateTime = new File(res.getFile()).lastModified();
			if (updateTime != lastFileUpdateTime) {
				lastFileUpdateTime = updateTime;
				flag = true;
			}
		}
		return flag;
	}

	public static String getStackTrace(Exception e) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		return sw.toString(); // stack trace as a string
	}
}
