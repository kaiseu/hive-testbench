-- Settings for SparkSQL engine
set spark.default.parallelism=348;
set spark.sql.autoBroadcastJoinThreshold=524288000;
set spark.sql.shuffle.partitions=1000;
