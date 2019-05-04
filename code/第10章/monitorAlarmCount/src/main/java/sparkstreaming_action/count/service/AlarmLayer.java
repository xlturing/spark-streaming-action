package sparkstreaming_action.count.service;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import sparkstreaming_action.count.entity.Alarm;
import sparkstreaming_action.count.entity.Rule;
import sparkstreaming_action.count.util.MysqlUtils;

/**
 * 扫描统计层和报警规则层，发出报警
 * 
 * @author litaoxiao
 * 
 */
public class AlarmLayer {
	private static Logger log = Logger.getLogger(AlarmLayer.class);
	private CountLayer countLayer;

	public AlarmLayer(CountLayer countLayer) {
		this.countLayer = countLayer;
		log.warn("AlarmLayer init done!");
	}

	/**
	 * 根据统计层和规则层进行报警
	 */
	public void alarm() {
		// 遍历报警规则
		for (Entry<Integer, Map<Integer, Map<String, Integer>>> grwc : countLayer.gameRuleWordCount.entrySet()) {
			int gameId = grwc.getKey();
			for (Entry<Integer, Map<String, Integer>> rwc : grwc.getValue().entrySet()) {
				int ruleId = rwc.getKey();
				Rule rule = countLayer.idRule.get(ruleId);
				// 报警算法
				double sum = 0, count = 0;
				for (Entry<String, Integer> wc : rwc.getValue().entrySet()) {
					sum += wc.getValue();
					count += 1;
				}
				if (rule.type == 0)
					sum /= count;
				if (sum >= rule.threshold) {
					// 超过词频限制，进行报警
					Alarm alarm = new Alarm();
					alarm.game_id = gameId;
					alarm.game_name = rule.game_name;
					alarm.rule_id = ruleId;
					alarm.rule_name = rule.rule_name;
					alarm.has_sent = 0;
					alarm.is_problem = -1;
					alarm.words = rule.words;
					alarm.words_freq = map2Str(rwc.getValue());
					MysqlUtils.insert("alarms", alarm);
					log.warn(alarm.toString());
					// 更新词频统计数据到0
					for (String w : rwc.getValue().keySet()) {
						rwc.getValue().put(w, 0);
					}
				}
			}
		}
	}

	public String map2Str(Map<String, Integer> m) {
		StringBuilder sb = new StringBuilder();
		for (Entry<String, Integer> e : m.entrySet())
			sb.append(String.format("%s:%d ", e.getKey(), e.getValue()));
		return sb.toString();
	}
}
