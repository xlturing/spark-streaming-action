package sparkstreaming_action.count.service;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.log4j.Logger;

import sparkstreaming_action.count.entity.Record;
import sparkstreaming_action.count.util.TrashFilterUtils;

/**
 * 过滤层 1.垃圾过滤 2.重复项过滤
 * 
 * @author litaoxiao
 * 
 */
public class FilterLayer {
	private static Logger log = Logger.getLogger(FilterLayer.class);

	public FilterLayer() {
	}

	/**
	 * 过滤项
	 * 
	 * @param record
	 * @return
	 */
	public boolean filter(Record record) {
		// 是否为垃圾
		if (isTrash(record)) {
			log.warn("[filterTrash] " + record);
			try {
				FileUtils.writeStringToFile(
						new File("filter/" + "trashFilter" + DateFormatUtils.format(new Date(), "yyyy-MM-dd")),
						record.toString(), true);
			} catch (IOException e) {
				log.error("[filterTrashWrite] error!", e);
			}
			return true;
		}
		return false;
	}

	/**
	 * 判断是否是垃圾项
	 * 
	 * @param record
	 * @return
	 */
	private boolean isTrash(Record record) {
		return TrashFilterUtils.isTrash(record.review);
	}

	/**
	 * 过滤HTML/Script/Style标签
	 * 
	 * @param htmlStr
	 * @return
	 */
	public static String delHTMLTag(String htmlStr) {
		String regEx_script = "<script[^>]*?>[\\s\\S]*?<\\/script>"; // 定义script的正则表达式
		String regEx_style = "<style[^>]*?>[\\s\\S]*?<\\/style>"; // 定义style的正则表达式
		String regEx_html = "<[^>]+>"; // 定义HTML标签的正则表达式

		Pattern p_script = Pattern.compile(regEx_script, Pattern.CASE_INSENSITIVE);
		Matcher m_script = p_script.matcher(htmlStr);
		htmlStr = m_script.replaceAll(""); // 过滤script标签

		Pattern p_style = Pattern.compile(regEx_style, Pattern.CASE_INSENSITIVE);
		Matcher m_style = p_style.matcher(htmlStr);
		htmlStr = m_style.replaceAll(""); // 过滤style标签

		Pattern p_html = Pattern.compile(regEx_html, Pattern.CASE_INSENSITIVE);
		Matcher m_html = p_html.matcher(htmlStr);
		htmlStr = m_html.replaceAll(""); // 过滤html标签

		return htmlStr.trim(); // 返回文本字符串
	}

	public static void main(String[] args) {
		String s = "<p>你好阿，这游戏真的不错</p><img src='http://11213sdfasf.jpg' />";
		System.out.println(delHTMLTag(s));
	}
}
