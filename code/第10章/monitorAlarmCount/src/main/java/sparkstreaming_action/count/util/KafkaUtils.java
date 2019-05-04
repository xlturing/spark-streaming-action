package sparkstreaming_action.count.util;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

public class KafkaUtils {
	static Logger log = Logger.getLogger(KafkaUtils.class);

	private static KafkaUtils instance = new KafkaUtils();
	private KafkaProducer<String, byte[]> producer = null;
	private KafkaConsumer<String, String> consumer = null;
	private String producerTopic = "";
	private boolean autoCommit = false; // 手动同步offset到zookeeper
	private Set<TopicPartition> assginedPartitions = null;

	private KafkaUtils() {
	}

	public static KafkaUtils getInstance() {
		return instance;
	}

	public KafkaProducer<String, byte[]> getProducer() {
		return producer;
	}

	public KafkaConsumer<String, String> getConsumer() {
		return consumer;
	}

	public String getProducerTopic() {
		return producerTopic;
	}

	public boolean initialize() {
		try {
			Properties propsConsumer = new Properties();

			// kafka original conf
			propsConsumer.put("bootstrap.servers", ConfigUtils.getConfig("bootstrap.servers"));

			// consumer special
			propsConsumer.put("group.id", ConfigUtils.getConfig("group.id"));
			propsConsumer.put("enable.auto.commit", autoCommit);
			propsConsumer.put("auto.offset.reset", ConfigUtils.getConfig("auto.offset.reset"));
			propsConsumer.put("session.timeout.ms", "35000");
			propsConsumer.put("max.partition.fetch.bytes", 64 * 1024 * 1024); // 64MB
			propsConsumer.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			propsConsumer.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			propsConsumer.put("max.poll.records", ConfigUtils.getConfig("max.poll.records"));

			consumer = new KafkaConsumer<String, String>(propsConsumer);

			// manually assign partitions. when assign partitions manually,
			// "subscribe" should not be used
			String topic = ConfigUtils.getConfig("consumer.topic");
			List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
			LinkedList<TopicPartition> topicPartitions = new LinkedList<TopicPartition>();
			for (PartitionInfo info : partitionInfos) {
				log.info("topic has partition:" + info.partition());
				TopicPartition topicPartition = new TopicPartition(topic, info.partition());
				topicPartitions.add(topicPartition);
			}
			consumer.assign(topicPartitions);

			// log initial partition positions
			assginedPartitions = consumer.assignment();
			log.info("Initial partition positions:");
			logPosition(false);

			// if seek to begin
			if (ConfigUtils.getBooleanValue("frombegin")) {
				consumer.seekToBeginning(topicPartitions);
				log.info("Seek to beginning is set, after seek to beginning, now partition positions:");
				logPosition(false);
			}
			// seek to end
			if (ConfigUtils.getBooleanValue("fromend")) {
				consumer.seekToEnd(topicPartitions);
				log.info("Seek to end is set, after seek to end, now partition positions:");
				logPosition(false);
			}

		} catch (Exception e) {
			log.error("initial KafkaUtils fails, e: " + ExceptionUtils.getStackTrace(e));
			return false;
		}
		return true;
	}

	public void tryCommit(ConsumerRecords<String, String> fetchedRecords, boolean needLog) {
		// only if the poll operation get messages, commit offsets
		if (fetchedRecords.count() > 0 && !autoCommit) {
			consumer.commitSync();
			if (needLog) {
				log.info("=== After commitSync, now partition positions:");
				logPosition(false);
			}
		}
	}

	private void logPosition(boolean debug) {
		for (TopicPartition assginedPartition : assginedPartitions) {
			if (debug) {
				log.debug(String.format("partition %d position: %d", assginedPartition.partition(),
						consumer.position(assginedPartition)));
			} else {
				log.info(String.format("partition %d position: %d", assginedPartition.partition(),
						consumer.position(assginedPartition)));
			}
		}
	}
}
