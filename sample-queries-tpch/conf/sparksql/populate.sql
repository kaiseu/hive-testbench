-- settings for populate metastore
--set hive.execution.engine=spark;
--set spark.home=/opt/dars/spark243_noPhive;
--set spark.master=yarn;
--set spark.deploy.mode=client;
--set spark.executor.cores=8;
--set spark.executor.memory=35g;
--set spark.yarn.executor.memoryOverhead=3072;
--set spark.driver.memory=8g;
--set spark.dynamicAllocation.enabled=true;
--set spark.shuffle.service.enabled=true;
--set spark.network.timeout=1200s;
--set spark.executor.extraJavaOptions=-XX:+UseParallelOldGC -XX:ParallelGCThreads=8 -XX:NewRatio=1 -XX:SurvivorRatio=1;
--set spark.serializer=org.apache.spark.serializer.KryoSerializer;
--set spark.io.compression.codec=lzf;
--set hive.exec.parallel=true;
--set hive.vectorized.execution.enabled=true;
--set hive.vectorized.execution.reduce.enabled=true;
--set hive.vectorized.execution.reduce.groupby.enabled=true;
--needed for populate table 
set hive.exec.dynamic.partition.mode=nonstrict;