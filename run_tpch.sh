## script for running TPC-H queries

################################################################################
## User Specific Settings, Change Based on Your Needs
################################################################################
## scale factor or data scale to run
SCALE_FACTOR="2"
## engine to run, can be mr spark sparksql
ENGINE="mr"
## file format, can be orc or parquet
FILEFORMAT="orc"
## whether to automatically clear cache before round run
CACHE_CLEAR="true"
## host names used for clear cache, usually is all the machines in a cluster
HOSTS="ubuntu16041"
## queries to run
QUERY_LIST="1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22"

################################################################################
## DO NOT NEED TO EDIT BELOW PARAS!!!
################################################################################
LOG_NAME="logs"
CURRENT_DIR=$( cd $( dirname ${BASH_SOURCE[0]} ) && pwd )
BENCH_SETTING="${CURRENT_DIR}/sample-queries-tpch/testbench.settings"
GLOBAL_SETTING="${CURRENT_DIR}/sample-queries-tpch/conf/${ENGINE}.settings"
LOCAL_SETTING_ROOT="${CURRENT_DIR}/sample-queries-tpch/conf/${ENGINE}"
PRINT_SETTING="${CURRENT_DIR}/sample-queries-tpch/conf/print.settings"
SPARKSQL_USER_CONF="${CURRENT_DIR}/sample-queries-tpch/conf/sparksql.conf"
QUERY_ROOT="${CURRENT_DIR}/sample-queries-tpch"
OUT_DIR_PATH="${CURRENT_DIR}/output"
RAW_DATA_DIR="/tmp/tpch-generate"
POPULATE_SETTING="${CURRENT_DIR}/sample-queries-tpch/conf/${ENGINE}/populate.settings"
# Tables in the TPC-H schema.
TABLES="part partsupp supplier customer orders lineitem nation region"
################################################################################
## DO NOT NEED TO EDIT ABOVE PARAS!!!
################################################################################


if test ${SCALE_FACTOR} -le 1000; then
	SCHEMA_TYPE=flat
else
	SCHEMA_TYPE=partitioned
fi
DATABASE=tpch_${SCHEMA_TYPE}_${FILEFORMAT}_${SCALE_FACTOR}

if [ ! -d ${OUT_DIR_PATH}/${LOG_NAME} ];then
	echo "Creating output dir: ${OUT_DIR_PATH}/${LOG_NAME}"
	mkdir -p ${OUT_DIR_PATH}/${LOG_NAME}
	echo "Dir Created!"
fi

BUCKETS=13
if [ "X$DEBUG_SCRIPT" != "X" ]; then
        set -x
fi

function usage(){
	echo "Usage: runQuery query_number"
	exit 1
}

#function getExecTime() {
#    start=$1
#    end=$2
#    start_s=$(echo $start | cut -d '.' -f 1)
#    start_ns=$(echo $start | cut -d '.' -f 2)
#    end_s=$(echo $end | cut -d '.' -f 1)
#    end_ns=$(echo $end | cut -d '.' -f 2)
#    delta_ms=$(( ( 10#$end_s - 10#$start_s ) * 1000 + ( 10#$end_ns / 1000000 - 10#$start_ns / 1000000 ) ))
#    show_s=$(( $delta_ms / 1000 ))
#    show_ms=$(( $delta_ms % 1000 ))
#    echo "++ Duration: ${show_s}s ${show_ms}ms ++"
#}
#
function DATE_PREFIX(){
        INFO_LEVEL=$1
        MESSAGE=$2
        echo -e "`date '+%Y-%m-%d %H:%M:%S'` ${INFO_LEVEL}  ${MESSAGE}"
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
        if [ ! -f ${CURRENT_DIR}/tpch-gen/target/tpch-gen-1.0-SNAPSHOT.jar ]; then
                echo "Please build the data generator with ./tpch-build.sh first"
                exit 1
        fi
        if [ ${SCALE_FACTOR} -eq 1 ]; then
                echo "Scale factor must be greater than 1"
                exit 1
        fi

	hdfs dfs -ls ${RAW_DATA_DIR}/${SCALE_FACTOR} > /dev/null 2>&1
	if [ $? == 0 ]; then
		echo "Clearing existing data dir ${RAW_DATA_DIR}/${SCALE_FACTOR} ..."
		hdfs dfs -rm -r -skipTrash ${RAW_DATA_DIR}/${SCALE_FACTOR} > /dev/null 2>&1
	fi
	#hdfs dfs -mkdir -p ${RAW_DATA_DIR}/${SCALE_FACTOR} > /dev/null
        hdfs dfs -ls ${RAW_DATA_DIR}/${SCALE_FACTOR} > /dev/null 2>&1
        if [ $? -ne 0 ]; then
		echo "Generating data at scale factor ${SCALE_FACTOR}."
                (cd tpch-gen; hadoop jar target/*.jar -d ${RAW_DATA_DIR}/${SCALE_FACTOR}/ -s ${SCALE_FACTOR})
        fi
        hdfs dfs -ls ${RAW_DATA_DIR}/${SCALE_FACTOR} > /dev/null 2>&1
        if [ $? -ne 0 ]; then
                echo "Data generation failed, exiting."
                exit 1
        fi

        hadoop fs -chmod -R 777  ${RAW_DATA_DIR}/${SCALE_FACTOR}
        echo "TPC-H text data generation complete."
}

function populateMetastore(){
        if [ "X${HIVE_HOME}" = "X" ]; then
                which hive > /dev/null 2>&1
                if [ $? -ne 0 ]; then
                        DATE_PREFIX "ERROR" "Script must be run where Hive is installed"
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
                DATABASE=tpch_${SCHEMA_TYPE}_${FILEFORMAT}_${SCALE_FACTOR}
        fi

        LOAD_FILE="${OUT_DIR_PATH}/${LOG_NAME}/logs_tpch_populatemetastore_${ENGINE}_${FILEFORMAT}_${SCALE_FACTOR}_`date +%Y%m%d%H%M%S`.log"
        SILENCE="2> /dev/null 1> /dev/null"
	if [ "X$DEBUG_SCRIPT" != "X" ]; then
                SILENCE=""
        fi

#        echo -e "all: ${TABLES}" > $LOAD_FILE

        # Create the text/flat tables as external tables. These will be later be converted to ORCFile.
	DATE_PREFIX "INFO" "Start populating metastore..." 2>&1 | tee ${LOAD_FILE} 
        DATE_PREFIX "INFO" "Loading text data into external tables..." 2>&1 | tee -a ${LOAD_FILE}
        runcommand "$HIVE  -i settings/load-flat.sql -f ddl-tpch/bin_flat/alltables.sql --hivevar DB=tpch_text_${SCALE_FACTOR} --hivevar LOCATION=${RAW_DATA_DIR}/${SCALE_FACTOR}" 2>&1 | tee -a ${LOAD_FILE}
        i=1
        total=8
        MAX_REDUCERS=2600 # maximum number of useful reducers for any scale
        REDUCERS=$((test ${SCALE_FACTOR} -gt ${MAX_REDUCERS} && echo ${MAX_REDUCERS}) || echo ${SCALE_FACTOR})
	## if populate configuration file exists add it to the command
	OPTION=()
	if [[ -e ${POPULATE_SETTING} ]]; then
		OPTION+=(-i ${POPULATE_SETTING})
	fi
        # Populate the tables.
	start=$(date +%s%3N)
        for t in ${TABLES}
        do
		DATE_PREFIX "INFO" "Populating data for table: ${t}" 2>&1 | tee -a ${LOAD_FILE}
		
                COMMAND="$HIVE ${OPTION[@]} -f ddl-tpch/bin_${SCHEMA_TYPE}/${t}.sql --hivevar DB=${DATABASE} --hivevar SOURCE=tpch_text_${SCALE_FACTOR} --hivevar SCALE=${SCALE_FACTOR} --hivevar REDUCERS=${REDUCERS} --hivevar FILE=${FILEFORMAT}"
                DATE_PREFIX "INFO" "The command is: ${COMMAND}" 2>&1 | tee -a $LOAD_FILE
		${COMMAND} 2>&1 | tee -a $LOAD_FILE
		RES=${PIPESTATUS[0]}
		if [[ ${RES} -ne 0 ]]; then
			DATE_PREFIX "ERROR" "Populating data for table: ${t} FAILED! Exiting..." 2>&1 | tee -a $LOAD_FILE
			exit -2
		else
			DATE_PREFIX "INFO" "Finished table: $t ($i/$total)." 2>&1 | tee -a $LOAD_FILE
		fi
                i=`expr $i + 1`
        done
	end=$(date +%s%3N)
	getExecTime $start $end 2>&1 | tee -a $LOAD_FILE
        DATE_PREFIX "INFO" "Data loaded into database ${DATABASE}." 2>&1 | tee -a $LOAD_FILE
}

## run a single query
function runQuery(){
	if [[ $# != 1 ]]; then
		usage;
	fi
	
	if [[ ! -d ${OUT_DIR_PATH}/${LOG_NAME} ]];then
        	DATE_PREFIX "INFO" "Creating output dir: ${OUT_DIR_PATH}/${LOG_NAME}"
       		mkdir -p ${OUT_DIR_PATH}/${LOG_NAME}
        	DATE_PREFIX "INFO" "Dir Created!"
	fi
	
	OPTION=(-i ${BENCH_SETTING}) 

	LOCAL_SETTING="${CURRENT_DIR}/sample-queries-tpch/conf/${ENGINE}/tpch_query${1}_${ENGINE}.settings"
	
	if [[ ${ENGINE} == "mr" || ${ENGINE} == "spark" ]]; then
	        if [ -e ${GLOBAL_SETTING} ]; then
			sed -i "/hive.execution.engine/d" ${GLOBAL_SETTING} ## delete line that contians hive.execution.engine
			sed -i "1a\set hive.execution.engine=${ENGINE};" ${GLOBAL_SETTING} ## add set hive.execution.engine=${ENGINE}; after the 1st line
			OPTION+=(-i ${GLOBAL_SETTING})
	        else
			echo "-- settings for ${ENGINE} engine" > ${GLOBAL_SETTING}
			echo "set hive.execution.engine=${ENGINE};" >> ${GLOBAL_SETTING}
			OPTION+=(-i ${GLOBAL_SETTING})
	        fi

        	if [ -e ${LOCAL_SETTING} ]; then
                	OPTION+=(-i ${LOCAL_SETTING})
        	fi
		OPTION+=(-f ${QUERY_ROOT}/tpch_query${1}.sql --database ${DATABASE})
		## keep print setting at last
        	if [ -e ${PRINT_SETTING} ]; then
                	OPTION+=(-i ${PRINT_SETTING})
        	fi
		CMD="hive ${OPTION[@]}"
	elif [[ ${ENGINE} == "sparksql" ]]; then
		if [ -e ${SPARKSQL_USER_CONF} ]; then
                	OPTION+=(--properties-file ${SPARKSQL_USER_CONF})
        	fi
		if [ -e ${GLOBAL_SETTING} ]; then
			OPTION+=(-i ${GLOBAL_SETTING})
		fi
		if [ -e ${LOCAL_SETTING} ]; then
                	OPTION+=(-i ${LOCAL_SETTING})
        	fi
		OPTION+=(-f ${QUERY_ROOT}/tpch_query${1}.sql --database ${DATABASE})
		## keep print setting at last
        	if [ -e ${PRINT_SETTING} ]; then
                	OPTION+=(-i ${PRINT_SETTING})
        	fi
		CMD="spark-sql ${OPTION[@]}"
	else
		DATE_PREFIX "ERROR" "Currently only support engine: mr/spark/sparksql, exiting..."
		exit -1
	fi
	
	DATE_PREFIX "INFO" "Running query$1 with command: ${CMD}" 2>&1 | tee ${OUT_DIR_PATH}/${LOG_NAME}/tpch_query${1}.log
	start=$(date +%s%3N)
	${CMD} 2>&1 | tee -a ${OUT_DIR_PATH}/${LOG_NAME}/tpch_query${1}.log
	RES=${PIPESTATUS[0]}
	end=$(date +%s%3N)
	getExecTime $start $end 2>&1 | tee -a ${OUT_DIR_PATH}/${LOG_NAME}/tpch_query${1}.log
	if [[ ${RES} == 0 ]]; then
		DATE_PREFIX "INFO" "query$1 finished successfully!" 2>&1 | tee -a ${OUT_DIR_PATH}/${LOG_NAME}/tpch_query${1}.log
	else
		DATE_PREFIX "ERROR" "query$1 failed!" 2>&1 | tee -a ${OUT_DIR_PATH}/${LOG_NAME}/tpch_query${1}.log
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
		DATE_PREFIX "INFO" "Running round $r"
		export LOG_NAME=logs_tpch_${ENGINE}_${FILEFORMAT}_${SCALE_FACTOR}_`date +%Y%m%d%H%M%S`
		for q in ${QUERY_LIST};
		do
			runQuery $q
		done
		
		DATE_PREFIX "INFO" "Round $r finished, logs are saved into: ${OUT_DIR_PATH}/${LOG_NAME}"
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
                        DATE_PREFIX "WARNING" "Clear cache is chosen but hosts file does not exists, will not clear cache!"
                fi
        else
                DATE_PREFIX "WARNING" "Will not automatically clear cache!"
        fi
}

################################################################################
## Start From Here!
################################################################################
#dataGen
populateMetastore
#runQuery 1
runAll 1
#runQuery 2
