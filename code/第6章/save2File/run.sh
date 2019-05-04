/Users/xiaolitao/Tools/spark-2.2.0-bin-hadoop2.7/bin/spark-submit \
	--class sparkstreaming_action.save2file.main.Save2File \
	--num-executors 4 \
	--driver-memory 1G \
	--executor-memory 1g  \
	--executor-cores 1 \
	--conf spark.default.parallelism=1000 \
	target/Save2File_SparkStreaming-0.1-jar-with-dependencies.jar \
	/Users/xiaolitao/Program/scala/Save2File_SparkStreaming
