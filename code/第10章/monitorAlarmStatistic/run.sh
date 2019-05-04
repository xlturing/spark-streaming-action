nohup /Users/xiaolitao/Tools/spark-2.2.0-bin-hadoop2.7/bin/spark-submit \
	--class sparkstreaming_action.alarm.main.MonitorAlarmStatistic \
	--num-executors 4 \
	--driver-memory 1G \
	--executor-memory 1g  \
	--executor-cores 1 \
	--conf spark.default.parallelism=1000 \
	target/MonitorAlarmStatistic-0.1-jar-with-dependencies.jar &
