package sparkstreaming_action.count.main;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import com.google.gson.Gson;

import sparkstreaming_action.count.entity.Record;
import sparkstreaming_action.count.service.AlarmLayer;
import sparkstreaming_action.count.service.CountLayer;
import sparkstreaming_action.count.service.FilterLayer;
import sparkstreaming_action.count.util.CommonUtils;
import sparkstreaming_action.count.util.ConfigUtils;
import sparkstreaming_action.count.util.KafkaUtils;
import sparkstreaming_action.count.util.MysqlUtils;
import sparkstreaming_action.count.util.TimeUtils;
import sparkstreaming_action.count.util.TrashFilterUtils;

public class ConsumerMain {
	private static Logger log = Logger.getLogger(ConsumerMain.class);
	private static Gson gson = new Gson();
	private Long nextReloadTime;
	private Long lastUpdateTime = 0L; // 规则表上次更新时间
	private Long lastGamesUpdateTime = 0L;// 监控游戏上次更新时间
	private Long kafkaLogTimes = 0L;

	// 过滤层
	private FilterLayer filterLayer;
	// 统计层
	private CountLayer countLayer;
	// 报警层
	private AlarmLayer alarmLayer;

	public ConsumerMain(long beginTime) {
		filterLayer = new FilterLayer();
		countLayer = new CountLayer();
		alarmLayer = new AlarmLayer(countLayer);
		long cur = TimeUtils.currentTimeSeconds();
		nextReloadTime = cur + ConfigUtils.getIntValue("reload_interval");

		lastUpdateTime = MysqlUtils.getUpdateTime("rules");
		lastGamesUpdateTime = MysqlUtils.getUpdateTime("monitor_games");
	}

	public void run() {
		// kafka接收数据层
		KafkaUtils kafkaUtils = KafkaUtils.getInstance();
		if (!kafkaUtils.initialize()) {
			log.error("kafka init error! exit!");
			System.exit(-1);
		}
		KafkaConsumer<String, String> consumer = kafkaUtils.getConsumer();
		long count = 0;
		// 消费者任务
		while (true) {
			try {
				// 统计
				ConsumerRecords<String, String> records = consumer.poll(200);
				for (ConsumerRecord<String, String> record : records) {
					if (count++ % 100000 == 0) {
						log.warn("[CurDataCount] count: " + count);
					}

					Record r = gson.fromJson(record.value(), Record.class);
					// ignore filter data
					if (filterLayer.filter(r))
						continue;
					countLayer.addRecord(r);
				}
				// 报警
				alarmLayer.alarm();

				if (kafkaLogTimes++ % 10 == 0) {
					kafkaUtils.tryCommit(records, true);
				} else {
					kafkaUtils.tryCommit(records, false);
				}

				// 重新加载
				if (nextReloadTime <= TimeUtils.currentTimeSeconds()) {
					long updateTime = MysqlUtils.getUpdateTime("rules");
					long gamesUpdateTime = MysqlUtils.getUpdateTime("monitor_games");
					if (updateTime != lastUpdateTime || gamesUpdateTime != lastGamesUpdateTime) {
						log.warn("rules or games changed!");
						countLayer.reload();
						lastUpdateTime = updateTime;
						lastGamesUpdateTime = gamesUpdateTime;
					}
					// 垃圾过滤个规则直接reload
					if (CommonUtils.isFileChange("patterns_appstore.txt", "patterns_forum.txt")) {
						TrashFilterUtils.reload();
					}
					while (nextReloadTime <= TimeUtils.currentTimeSeconds())
						nextReloadTime += ConfigUtils.getIntValue("reload_interval");
				}
			} catch (Exception e) {
				log.error("main error:" + CommonUtils.getStackTrace(e));
			}
		}
	}

	public static void main(String[] args) {
		final ConsumerMain consumerMain = new ConsumerMain(TimeUtils.currentTimeSeconds());
		log.warn("All things init done!");
		consumerMain.run();
	}
}
