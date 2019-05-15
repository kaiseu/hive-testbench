-- settings for MR engine
set hive.execution.engine=mr;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=1000000000;
set hive.exec.parallel=true;
set hive.exec.reducers.max=352;
