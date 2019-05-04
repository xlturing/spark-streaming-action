package sparkstreaming_action.count.entity;

/**
 * Spark过滤后的json记录
 * 
 * @author litaoxiao
 * 
 */
public class Record {
	public String review;
	public String reviewSeg;
	public int gameId;
	public String gameName;

	@Override
	public String toString() {
		return String.format("gameId: %d\tgameName: %s\treview:%s\nreviewSeg:%s", gameId, gameName, review,
				reviewSeg);
	}
}
