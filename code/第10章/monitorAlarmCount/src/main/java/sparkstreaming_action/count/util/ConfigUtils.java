package sparkstreaming_action.count.util;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

public class ConfigUtils {
	final static String resourceFullName = "/config.properties";
	final static Properties props = new Properties();

	static {
		InputStreamReader is = null;
		try {
			is = new InputStreamReader(ConfigUtils.class.getResourceAsStream(resourceFullName), "utf-8");
			props.load(is);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * get config String value by key
	 * 
	 * @param key
	 * @return
	 */
	public static String getConfig(String key) {
		return props.getProperty(key);
	}

	/**
	 * get config Boolean value by key(true/false)
	 * 
	 * @param key
	 * @return
	 */
	public static boolean getBooleanValue(String key) {
		if (props.get(key).equals("true")) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * get config Integer value by key
	 * 
	 * @param key
	 * @return
	 */
	public static Integer getIntValue(String key) {
		return Integer.parseInt(getConfig(key));
	}
}
