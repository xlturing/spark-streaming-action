nohup /Users/xiaolitao/Tools/spark-2.2.0-bin-hadoop2.7/bin/spark-submit \
	--class sparkstreaming_action.log.analysis.LogAnalysis \
	--num-executors 4 \
	--driver-memory 1G \
	--executor-memory 1g  \
	--executor-cores 1 \
	--conf spark.default.parallelism=1000 \
	--driver-class-path /Users/xiaolitao/.m2/repository/mysql/mysql-connector-java/5.1.31/mysql-connector-java-5.1.31.jar \
	target/logAnalysis-0.1-jar-with-dependencies.jar &
