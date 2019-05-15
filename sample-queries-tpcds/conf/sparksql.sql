-- Settings for SparkSQL engine
set spark.default.parallelism=288;
set spark.sql.autoBroadcastJoinThreshold=1073741824;
set spark.sql.shuffle.partitions=288;
