/Users/xiaolitao/Tools/spark-2.2.0-bin-hadoop2.7/bin/spark-submit \
	--class sparkstreaming_action.rdd.operation.RDDOperation \
	--num-executors 4 \
	--driver-memory 1G \
	--executor-memory 1g  \
	--executor-cores 1 \
	--conf spark.default.parallelism=1000 \
	target/rddOperation-0.1-jar-with-dependencies.jar
