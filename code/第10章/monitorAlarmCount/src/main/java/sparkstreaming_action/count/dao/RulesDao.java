package sparkstreaming_action.count.dao;

import java.util.List;

import sparkstreaming_action.count.entity.Rule;
import sparkstreaming_action.count.util.MysqlUtils;

public class RulesDao {
	public static List<Rule> getGameRules() {
		return MysqlUtils.queryByBeanListHandler("select * from rules where state=0;", Rule.class);
	}
}
