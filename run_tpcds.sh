## script for running TPC-DS queries

################################################################################
## User Specific Settings, Change Based on Your Needs 
################################################################################
## scale factor or data scale to run
SCALE_FACTOR="1000"
## engine to run, can be mr spark sparksql
ENGINE="spark"
## file format, can be orc or parquet
FILEFORMAT="orc"
## whether to automatically clear cache before round run
CACHE_CLEAR="true"
## host names used for clear cache, usually is all the machines in a cluster
HOSTS="r73-master r73-slave1 r73-slave2 r73-slave3 r73-slave4"
## queries to run
QUERY_LIST="3 7 12 15 17 18 19 20 21 25 26 27 28 29 31 32 34 39 40 42 43 45 46 49 50 51 52 54 55 56 58 60 63 66 68 71 73 75 76 79 80 82 84 85 87 88 89 90 91 92 93 94 96 97 98"

################################################################################
## DO NOT NEED TO EDIT BELOW PARAS!!!
################################################################################
LOG_NAME="logs"
CURRENT_DIR=$( cd $( dirname ${BASH_SOURCE[0]} ) && pwd )
BENCH_SETTING="${CURRENT_DIR}/sample-queries-tpcds/testbench.settings"
GLOBAL_SETTING="${CURRENT_DIR}/sample-queries-tpcds/conf/${ENGINE}.settings"
LOCAL_SETTING_ROOT="${CURRENT_DIR}/sample-queries-tpcds/conf/${ENGINE}"
PRINT_SETTING="${CURRENT_DIR}/sample-queries-tpcds/conf/print.settings"
SPARKSQL_USER_CONF="${CURRENT_DIR}/sample-queries-tpcds/conf/sparksql.conf"
QUERY_ROOT="${CURRENT_DIR}/sample-queries-tpcds"
OUT_DIR_PATH="${CURRENT_DIR}/output"
RAW_DATA_DIR="/tmp/tpcds-generate"
# Tables in the TPC-DS schema.
DIMS="date_dim time_dim item customer customer_demographics household_demographics customer_address store promotion warehouse ship_mode reason income_band call_center web_page catalog_page web_site"
FACTS="store_sales store_returns web_sales web_returns catalog_sales catalog_returns inventory"
################################################################################
## DO NOT NEED TO EDIT ABOVE PARAS!!!
################################################################################


if test ${SCALE_FACTOR} -le 1000; then
	SCHEMA_TYPE=flat
else
	SCHEMA_TYPE=partitioned
fi
DATABASE=tpcds_${SCHEMA_TYPE}_${FILEFORMAT}_${SCALE_FACTOR}

if [[ ! -d ${OUT_DIR_PATH}/${LOG_NAME} ]];then
	echo "Creating output dir: ${OUT_DIR_PATH}/${LOG_NAME}"
	mkdir -p ${OUT_DIR_PATH}/${LOG_NAME}
	echo "Dir Created!"
fi

if [ "X$BUCKET_DATA" != "X" ]; then
        BUCKETS=13
        RETURN_BUCKETS=13
else
        BUCKETS=1
        RETURN_BUCKETS=1
fi
if [ "X$DEBUG_SCRIPT" != "X" ]; then
        set -x
fi

function usage(){
	echo "Usage: runQuery query_number"
	exit 1
}

function runcommand {
        if [ "X$DEBUG_SCRIPT" != "X" ]; then
                $1
        else
                $1 2>/dev/null
        fi
}

function getExecTime() {
	start=$1
	end=$2
	time_s=`echo "scale=3;$(($end-$start))/1000" | bc`
	echo "Duration: ${time_s} s"
}

function dataGen(){
	if [ ! -f ${CURRENT_DIR}/tpcds-gen/target/tpcds-gen-1.0-SNAPSHOT.jar ]; then
	        echo "Please build the data generator with ./tpcds-build.sh first"
        	exit 1
	fi
	if [ ${SCALE_FACTOR} -eq 1 ]; then
        	echo "Scale factor must be greater than 1"
        	exit 1
	fi

	hdfs dfs -mkdir -p ${RAW_DATA_DIR}
	hdfs dfs -ls ${RAW_DATA_DIR}/${SCALE_FACTOR} > /dev/null
	if [ $? -ne 0 ]; then
	       echo "Generating data at scale factor ${SCALE_FACTOR}."
	        (cd tpcds-gen; hadoop jar target/*.jar -d ${RAW_DATA_DIR}/${SCALE_FACTOR}/ -s ${SCALE_FACTOR})
	fi
	hdfs dfs -ls ${RAW_DATA_DIR}/${SCALE_FACTOR} > /dev/null
	if [ $? -ne 0 ]; then
        	echo "Data generation failed, exiting."
        	exit 1
	fi

	hadoop fs -chmod -R 777  /${RAW_DATA_DIR}/${SCALE_FACTOR}
	echo "TPC-DS text data generation complete."
}

function populateMetastore(){
	if [ "X${HIVE_HOME}" = "X" ]; then
		which hive > /dev/null 2>&1
		if [ $? -ne 0 ]; then
        		echo "Script must be run where Hive is installed"
        		exit 1
		else
			HIVE=hive
		fi
	else
		HIVE="${HIVE_HOME}/bin/hive"
	fi

	# Create the partitioned and bucketed tables.
	if [ "X${FILEFORMAT}" = "X" ]; then
        	FILEFORMAT=orc
	fi
	if [ "X${DATABASE}" = "X" ]; then
                DATABASE=tpcds_${SCHEMA_TYPE}_${FILEFORMAT}_${SCALE_FACTOR}
        fi

	LOAD_FILE="${OUT_DIR_PATH}/${LOG_NAME}/logs_tpcds_populatemetastore_${ENGINE}_${FILEFORMAT}_${SCALE_FACTOR}_`date +%Y%m%d%H%M%S`.log"
	SILENCE="2> /dev/null 1> /dev/null"
	if [ "X$DEBUG_SCRIPT" != "X" ]; then
        	SILENCE=""
	fi

	echo -e "all: ${DIMS} ${FACTS}" > $LOAD_FILE

	# Create the text/flat tables as external tables. These will be later be converted to ORCFile.
	echo "Loading text data into external tables."
	runcommand "$HIVE  -i settings/load-flat.sql -f ddl-tpcds/text/alltables.sql --hivevar DB=tpcds_text_${SCALE_FACTOR} --hivevar LOCATION=${RAW_DATA_DIR}/${SCALE_FACTOR}"

	i=1
	total=24
	MAX_REDUCERS=2500 # maximum number of useful reducers for any scale
	REDUCERS=$((test ${SCALE_FACTOR} -gt ${MAX_REDUCERS} && echo ${MAX_REDUCERS}) || echo ${SCALE_FACTOR})

	# Populate the smaller tables.
	for t in ${DIMS}
	do
        	COMMAND="$HIVE  -i settings/load-partitioned.sql -f ddl-tpcds/bin_partitioned/${t}.sql \
            	--hivevar DB=${DATABASE} --hivevar SOURCE=tpcds_text_${SCALE_FACTOR} \
            	--hivevar SCALE=${SCALE_FACTOR} \
            	--hivevar REDUCERS=${REDUCERS} \
            	--hivevar FILE=${FILEFORMAT}"
        	echo -e "${t}:\n\t@$COMMAND $SILENCE && echo 'Optimizing table $t ($i/$total).'" >> $LOAD_FILE
        	i=`expr $i + 1`
	done

	for t in ${FACTS}
	do
        	COMMAND="$HIVE  -i settings/load-partitioned.sql -f ddl-tpcds/bin_partitioned/${t}.sql \
            	--hivevar DB=${DATABASE} \
            	--hivevar SCALE=${SCALE_FACTOR} \
            	--hivevar SOURCE=tpcds_text_${SCALE_FACTOR} --hivevar BUCKETS=${BUCKETS} \
            	--hivevar RETURN_BUCKETS=${RETURN_BUCKETS} --hivevar REDUCERS=${REDUCERS} --hivevar FILE=${FILEFORMAT}"
        	echo -e "${t}:\n\t@$COMMAND $SILENCE && echo 'Optimizing table $t ($i/$total).'" >> $LOAD_FILE
        	i=`expr $i + 1`
	done

	make -j 1 -f $LOAD_FILE

	echo "Data loaded into database ${DATABASE}."
}


## run a single query
function runQuery(){
	if [[ $# != 1 ]]; then
		usage;
	fi
	
	if [[ ! -d ${OUT_DIR_PATH}/${LOG_NAME} ]];then
        	echo "Creating output dir: ${OUT_DIR_PATH}/${LOG_NAME}"
       		mkdir -p ${OUT_DIR_PATH}/${LOG_NAME}
        	echo "Dir Created!"
	fi
	
	OPTION=(-i ${BENCH_SETTING}) 
	if [ -e ${GLOBAL_SETTING} ]; then
		OPTION+=(-i ${GLOBAL_SETTING})
	fi

	LOCAL_SETTING="${CURRENT_DIR}/sample-queries-tpcds/conf/${ENGINE}/tpcds_query${1}_${ENGINE}.settings"
	
	if [[ ${ENGINE} == "mr" || ${ENGINE} == "spark" ]]; then
        	if [ -e ${LOCAL_SETTING} ]; then
                	OPTION+=(-i ${LOCAL_SETTING})
        	fi
		OPTION+=(-f ${QUERY_ROOT}/tpcds_query${1}.sql --database ${DATABASE})
		## keep print setting at last
        	if [ -e ${PRINT_SETTING} ]; then
                	OPTION+=(-i ${PRINT_SETTING})
        	fi
		CMD="hive ${OPTION[@]}"
	elif [[ ${ENGINE} == "sparksql" ]]; then
		if [ -e ${SPARKSQL_USER_CONF} ]; then
                	OPTION+=(--properties-file ${SPARKSQL_USER_CONF})
        	fi
		
		if [ -e ${LOCAL_SETTING} ]; then
                	OPTION+=(-i ${LOCAL_SETTING})
        	fi
		OPTION+=(-f ${QUERY_ROOT}/tpcds_query${1}.sql --database ${DATABASE})
		## keep print setting at last
        	if [ -e ${PRINT_SETTING} ]; then
                	OPTION+=(-i ${PRINT_SETTING})
        	fi
		CMD="spark-sql ${OPTION[@]}"
	else
		echo "Currently only support engine: mr/spark/sparksql, exiting..."
		exit -1
	fi
	
	echo "Running query$1 with command: ${CMD}" 2>&1 | tee ${OUT_DIR_PATH}/${LOG_NAME}/tpcds_query${1}.log
	start=$(date +%s%3N)
	eval ${CMD} 2>&1 | tee -a ${OUT_DIR_PATH}/${LOG_NAME}/tpcds_query${1}.log
	RES=$?
	end=$(date +%s%3N)
	getExecTime $start $end >> ${OUT_DIR_PATH}/${LOG_NAME}/tpcds_query${1}.log
	if [[ ${RES} == 0 ]]; then
		echo "query$1 finished successfully!" >> ${OUT_DIR_PATH}/${LOG_NAME}/tpcds_query${1}.log
	else
		echo "query$1 failed!" >> ${OUT_DIR_PATH}/${LOG_NAME}/tpcds_query${1}.log
	fi
}

## save logs and configuration file
function BACKUP(){
	SETTING_DIR=$1/conf
	if [[ ! -d ${SETTING_DIR} ]]; then
		mkdir -p ${SETTING_DIR}
	fi
	
	if [[ -d ${LOCAL_SETTING_ROOT} ]]; then
		cp -r ${LOCAL_SETTING_ROOT} ${SETTING_DIR}
	fi
	
	if [ -e ${BENCH_SETTING} ]; then
		cp -r ${BENCH_SETTING} ${SETTING_DIR}
	fi
	if [ -e ${GLOBAL_SETTING} ]; then
		cp -r ${GLOBAL_SETTING} ${SETTING_DIR}
	fi
	if [ -e ${PRINT_SETTING} ]; then
		cp -r ${PRINT_SETTING} ${SETTING_DIR}
	fi
	if [ -e ${SPARKSQL_USER_CONF} ]; then
                cp -r ${SPARKSQL_USER_CONF} ${SETTING_DIR}
        fi
}

## run all the queries defined in ${QUERY_LIST} one by one
function runAll(){
	if [[ $# != 1 ]]; then
                echo "Usage: runAll rounds_to_run"
		exit 2
        fi

	for ((r=1; r<=$1; r++))
	do
		clearCache
		echo "Running round $r"
		export LOG_NAME=logs_tpcds_${ENGINE}_${FILEFORMAT}_${SCALE_FACTOR}_`date +%Y%m%d%H%M%S`
		for q in ${QUERY_LIST};
		do
			runQuery $q
		done
		
		echo "Round $r finished, logs are saved into: ${OUT_DIR_PATH}/${LOG_NAME}"
		## Backup the corresponding settings to log dir
		BACKUP ${OUT_DIR_PATH}/${LOG_NAME}
	done
}

## clear the cache of machines defined in ${HOSTS}
function clearCache(){
	if [[ ${CACHE_CLEAR} == "true" ]] ; then
		if [ -e ${HOSTS} ]; then
			pssh -H ${HOSTS} -t 0  -i "sync; echo 3 > /proc/sys/vm/drop_caches && printf '\n%s\n' 'Ram-cache Cleared'"
			pssh -H ${HOSTS} -t 0  -i free -g
		else
			echo "Clear cache is chosen but hosts file does not exists, will not clear cache!"
		fi
	else
		echo "Will not automatically clear cache!"
	fi
}

################################################################################
## Start From Here!
################################################################################
runAll 1
#runQuery 2
