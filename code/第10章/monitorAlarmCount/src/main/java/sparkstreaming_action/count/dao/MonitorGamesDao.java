package sparkstreaming_action.count.dao;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.map.HashedMap;

import sparkstreaming_action.count.entity.MonitorGames;
import sparkstreaming_action.count.util.MysqlUtils;

public class MonitorGamesDao {
	/**
	 * 获取所有监视游戏
	 * 
	 * @return
	 */
	public static List<MonitorGames> getMonitorGames() {
		return MysqlUtils.queryByBeanListHandler("select * from monitor_games", MonitorGames.class);
	}

	/**
	 * 以[game_id -> monitorGame]的形式获取所有监视游戏
	 * 
	 * @return
	 */
	public static Map<Integer, MonitorGames> getMapMonitorGames() {
		Map<Integer, MonitorGames> mem = new HashedMap<>();

		for (MonitorGames monitorGame : MysqlUtils.queryByBeanListHandler("select * from monitor_games",
				MonitorGames.class)) {
			mem.put(monitorGame.game_id, monitorGame);
		}
		return mem;
	}
}
