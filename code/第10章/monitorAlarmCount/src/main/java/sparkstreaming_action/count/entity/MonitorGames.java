package sparkstreaming_action.count.entity;

/**
 * 监控的游戏库
 * 
 * @author litaoxiao
 * 
 */
public class MonitorGames {
	public int game_id;
	public String game_name;

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

}
