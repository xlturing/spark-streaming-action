package sparkstreaming_action.count.service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import sparkstreaming_action.count.dao.RulesDao;
import sparkstreaming_action.count.entity.Record;
import sparkstreaming_action.count.entity.Rule;
import sparkstreaming_action.count.util.ConfigUtils;

/**
 * 统计层面数据结构
 * 
 * @author litaoxiao
 * 
 */
public class CountLayer {
	private static Logger log = Logger.getLogger(CountLayer.class);
	public Map<Integer, Map<Integer, Map<String, Integer>>> gameRuleWordCount; // [gameId->[ruleId->[word->count]]]
	public Map<Integer, Rule> idRule; // [ruleId->rule]

	public CountLayer() {
		reload();
		log.warn("CountLayer init done!");
	}

	/**
	 * 将一条记录按照createTime进行统计
	 * 
	 * @param record
	 */
	public void addRecord(Record record) {
		int gameId = record.gameId;
		if (!gameRuleWordCount.containsKey(gameId)) {
			log.error("GameRuleWordCount don't contain gameId: " + gameId);
			return;
		}
		for (Entry<Integer, Map<String, Integer>> ruleWord : gameRuleWordCount.get(gameId).entrySet()) {
			int ruleId = ruleWord.getKey();
			for (Entry<String, Integer> wordCount : ruleWord.getValue().entrySet()) {
				String word = wordCount.getKey();
				if (isContain(record, word)) {
					gameRuleWordCount.get(gameId).get(ruleId).put(word, wordCount.getValue() + 1);
				}
			}
		}
	}

	/**
	 * 重新加载规则库和监控游戏库
	 */
	public void reload() {
		List<Rule> countRules = RulesDao.getGameRules();
		Map<Integer, Map<Integer, Map<String, Integer>>> newGameRuleWordCount = new HashMap<Integer, Map<Integer, Map<String, Integer>>>();
		idRule = new HashMap<Integer, Rule>();
		for (Rule rule : countRules) {
			idRule.put(rule.rule_id, rule);
			if (!newGameRuleWordCount.containsKey(rule.game_id))
				newGameRuleWordCount.put(rule.game_id, new HashMap<Integer, Map<String, Integer>>());
			if (!newGameRuleWordCount.get(rule.game_id).containsKey(rule.rule_id))
				newGameRuleWordCount.get(rule.game_id).put(rule.rule_id, new HashMap<String, Integer>());
			for (String word : rule.words.split(" ")) {
				if (gameRuleWordCount != null && gameRuleWordCount.containsKey(rule.game_id)
						&& gameRuleWordCount.get(rule.game_id).containsKey(rule.rule_id)
						&& gameRuleWordCount.get(rule.game_id).get(rule.rule_id).containsKey(word))
					newGameRuleWordCount.get(rule.game_id).get(rule.rule_id).put(word,
							gameRuleWordCount.get(rule.game_id).get(rule.rule_id).get(word));
				else
					newGameRuleWordCount.get(rule.game_id).get(rule.rule_id).put(word, 0);
			}
		}
		// 更新指针
		this.gameRuleWordCount = newGameRuleWordCount;
		log.warn("gameRuleWordCount reload done: " + gameRuleWordCount.size());
	}

	// 判断评论中是否含有该词 ，n元祖拼接匹配
	private boolean isContain(Record record, String word) {
		String[] segWords = record.reviewSeg.split("\t");
		for (int i = 0; i < segWords.length; i++) {
			for (int j = i; j < i + ConfigUtils.getIntValue("ngram") + 1 && j <= segWords.length; j++) {
				String mkWord = StringUtils.join(Arrays.copyOfRange(segWords, i, j), "");
				if (word.equals(mkWord))
					return true;
			}
		}
		return false;
	}
}
