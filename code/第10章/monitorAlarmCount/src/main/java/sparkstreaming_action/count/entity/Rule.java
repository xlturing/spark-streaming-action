package sparkstreaming_action.count.entity;

public class Rule {
	public int rule_id;
	public String rule_name;
	public int game_id;
	public String game_name;
	public int threshold;
	public String words;
	public int type; // 0按词平均值，1按词之和
	public int state; // 0规则有效 1规则失效

	public Rule() {
	}

	public Rule(Rule rule) {
		this.game_id = rule.game_id;
		this.game_name = rule.game_name;
		this.rule_id = rule.rule_id;
		this.rule_name = rule.rule_name;
		this.threshold = rule.threshold;
		this.type = rule.type;
		this.words = rule.words;
	}

	public int getRule_id() {
		return rule_id;
	}

	public void setRule_id(int rule_id) {
		this.rule_id = rule_id;
	}

	public String getRule_name() {
		return rule_name;
	}

	public void setRule_name(String rule_name) {
		this.rule_name = rule_name;
	}

	public int getGame_id() {
		return game_id;
	}

	public void setGame_id(int game_id) {
		this.game_id = game_id;
	}

	public String getGame_name() {
		return game_name;
	}

	public void setGame_name(String game_name) {
		this.game_name = game_name;
	}

	public int getThreshold() {
		return threshold;
	}

	public void setThreshold(int threshold) {
		this.threshold = threshold;
	}

	public String getWords() {
		return words;
	}

	public void setWords(String words) {
		this.words = words;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public int getState() {
		return state;
	}

	public void setState(int state) {
		this.state = state;
	}

	@Override
	public String toString() {
		return String.format("rule_id:%d\trule_name:%s\tgameid:%d\tgamename:%s\ttype:%d\twords:(%s)\tthreashold:(%s)\n",
				rule_id, rule_name, game_id, game_name, type, words, threshold);
	}

}
