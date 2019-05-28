## script for running TPC-DS queries

################################################################################
## User Specific Settings, Change Based on Your Needs 
################################################################################
## benchmark to run, cna be tpcds or tpch
BENCHMARK="tpcds"
## scale factor or data scale to run
SCALE_FACTOR="1000"
## engine to run, can be mr spark sparksql
ENGINE="sparksql"
## file format, can be orc or parquet
FILEFORMAT="parquet"
## whether to automatically clear cache before round run
CACHE_CLEAR="true"
## host names used for clear cache, usually is all the machines in a cluster
HOSTS="clr-node1 clr-node2 clr-node3 clr-node4"
## queries to run
#DS_QUERY_LIST="3 7 12 15 17 18 19 20 21 25 26 27 28 29 31 32 34 39 40 42 43 45 46 49 50 51 52 54 55 56 58 60 63 66 68 71 73 75 76 79 80 82 84 85 87 88 89 90 91 92 93 94 96 97 98"
DS_QUERY_LIST="1 2 3 4 5 6 7 8 9 10 11 12 13 14a 14b 15 16 17 18 19 20 21 22 23a 23b 24a 24b 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39a 39b 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 95 96 97 98 99"
H_QUERY_LIST="1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22"
################################################################################
## DO NOT NEED TO EDIT BELOW PARAS!!!
################################################################################
LOG_NAME="logs"
CURRENT_DIR=$( cd $( dirname ${BASH_SOURCE[0]} ) && pwd )
SETTING_ROOT="${CURRENT_DIR}/sample-queries-${BENCHMARK}/conf"
POPULATE_SETTING="${SETTING_ROOT}/${ENGINE}/populate.sql"
ANALYZE_SQL="${CURRENT_DIR}/ddl-${BENCHMARK}/bin_partitioned/analyze.sql"
BENCH_SETTING="${SETTING_ROOT}/${BENCHMARK}.sql"
GLOBAL_SETTING="${SETTING_ROOT}/${ENGINE}.sql"
LOCAL_SETTING_ROOT="${SETTING_ROOT}/${ENGINE}"
PRINT_SETTING="${SETTING_ROOT}/print.sql"
SPARKSQL_USER_CONF="${SETTING_ROOT}/sparksql/sparksql.conf"
QUERY_ROOT="${CURRENT_DIR}/sample-queries-${BENCHMARK}"
OUT_DIR_PATH="${CURRENT_DIR}/output"
RAW_DATA_DIR="/tmp/${BENCHMARK}-generate"
# Tables in the TPC-DS schema.
DIMS="date_dim time_dim item customer customer_demographics household_demographics customer_address store promotion warehouse ship_mode reason income_band call_center web_page catalog_page web_site"
FACTS="store_sales store_returns web_sales web_returns catalog_sales catalog_returns inventory"
# Tables in the TPC-H schema.
H_TABLES="part partsupp supplier customer orders lineitem nation region"
# Total number of tables, default is tpcds 24 tables
TOTAL=24
# Table names for population, tpcds will populate DIMS and FACTS separately
TABLES=${DIMS}
# Default query list, default is tpcds query list
QUERY_LIST=${DS_QUERY_LIST}
# Default execution engine, for mr and spark it needs hive, for sparkqsl it will use spark-sql directly
EXECUTION_BINARY="hive"
################################################################################
## DO NOT NEED TO EDIT ABOVE PARAS!!!
################################################################################

if [ "X${BENCHMARK}" = "Xtpcds" ]; then
	QUERY_LIST=${DS_QUERY_LIST}
	TABLES=${DIMS}
	TOTAL=24
elif [ "X${BENCHMARK}" = "Xtpch" ]; then
	QUERY_LIST=${H_QUERY_LIST}
	TABLES=${H_TABLES}
	TOTAL=8
else
	echo "Benchmark currently only tpcds and tpch are supported!"
	exit -1;
fi

if test ${SCALE_FACTOR} -lt 1000; then
	SCHEMA_TYPE=flat
else
	SCHEMA_TYPE=partitioned
fi
DATABASE=${BENCHMARK}_${SCHEMA_TYPE}_${FILEFORMAT}_${SCALE_FACTOR}

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

## Merge files into one file and remove duplicated lines
function Merge_conf() {
	if [ $# -lt 3 ]; then
		DATE_PREFIX "ERROR" "Merge configuration files failed, at least three inputs are needed!"	
		DATE_PREFIX "WARN" "Usage: Merge_conf file1 file2 file3 ... fileN result"
		exit -6
	fi
	tmp_file=/tmp/merge.tmp
	echo "## Auto generated configuration file"> ${tmp_file}
	until [ $# -eq 1 ]
	do
		cat $1 >> ${tmp_file}
		shift
	done
	sed -i '/^#/ d' ${tmp_file} ## delect line start with '#'
	sed -i '/^--/ d' ${tmp_file} ## delect line start with '--'
	sed -i 's/^set//g' ${tmp_file} ## delect 'set' start in a line
        sed -i 's/^ //g' ${tmp_file} ## delect ' ' start in a line
        sed -i 's/;$//g' ${tmp_file} ## delect ';' end in a line
        sed -i '/^$/d' ${tmp_file} ## delect blank lines

	OUT=$1 ## the last is the output file
	cat ${tmp_file} | sort -t '=' -k 1,1 -b -d -u -o ${OUT} ## sort by first column split by '=', ignore leading blanks and by dictionary-order
	#rm -f ${tmp_file}
}

function dataGen(){
	if [ ! -f ${CURRENT_DIR}/${BENCHMARK}-gen/target/${BENCHMARK}-gen-1.0-SNAPSHOT.jar ]; then
	        DATE_PREFIX "ERROR" "Please build the data generator with ./${BENCHMARK}-build.sh first"
        	exit 1
	fi
	if [ ${SCALE_FACTOR} -eq 1 ]; then
        	DATE_PREFIX "ERROR" "Scale factor must be greater than 1"
        	exit 1
	fi

	hdfs dfs -mkdir -p ${RAW_DATA_DIR}
	hdfs dfs -ls ${RAW_DATA_DIR}/${SCALE_FACTOR} 2>&1 > /dev/null
	if [ $? -ne 0 ]; then
	       DATE_PREFIX "INFO" "Generating data at scale factor ${SCALE_FACTOR}."
	        (cd ${BENCHMARK}-gen; hadoop jar target/*.jar -d ${RAW_DATA_DIR}/${SCALE_FACTOR}/ -s ${SCALE_FACTOR})
	fi
	hdfs dfs -ls ${RAW_DATA_DIR}/${SCALE_FACTOR} 2>&1 > /dev/null
	if [ $? -ne 0 ]; then
        	DATE_PREFIX "ERROR" "Data generation failed, exiting."
        	exit 1
	fi

	hadoop fs -chmod -R 777  ${RAW_DATA_DIR}/${SCALE_FACTOR}
	DATE_PREFIX "INFO" "${BENCHMARK} text data generation complete."
}

function populateMetastore(){
	DATE_PREFIX "INFO" "Populating metastore with engine: ${ENGINE}, the format is: ${FILEFORMAT}, scale factor is: ${SCALE_FACTOR}"
	if [[ "X${ENGINE}" = "Xmr" || "X${ENGINE}" = "Xspark" ]]; then
		if [ "X${HIVE_HOME}" = "X" ]; then
			which hive > /dev/null 2>&1
			if [ $? -ne 0 ]; then
	        		DATE_PREFIX "ERROR" "Script must be run where Hive is installed, or HIVE_HOME is set"
	        		exit 1
			else
				EXECUTION_BINARY="hive"
			fi
		else
			EXECUTION_BINARY="${HIVE_HOME}/bin/hive"
		fi
	elif [ "X${ENGINE}" = "Xsparksql" ]; then
		if [ "X${SPARK_HOME}" = "X" ]; then
			which spark-sql > /dev/null 2>&1
			if [ $? -ne 0 ]; then
				DATE_PREFIX "ERROR" "Script must be run where spark-sql is installed, or SPARK_HOME is set."
				exit 2
			else
				EXECUTION_BINARY="spark-sql"
			fi
		else
			EXECUTION_BINARY="${SPARK_HOME}/bin/spark-sql"
		fi
	fi
	EXECUTION_BINARY="${EXECUTION_BINARY} --properties-file ${SPARKSQL_USER_CONF}"

	# Create the partitioned and bucketed tables.
	if [ "X${FILEFORMAT}" = "X" ]; then
        	FILEFORMAT=orc
	fi
	if [ "X${DATABASE}" = "X" ]; then
                DATABASE=${BENCHMARK}_${SCHEMA_TYPE}_${FILEFORMAT}_${SCALE_FACTOR}
        fi

	POPULATE_LOG="${OUT_DIR_PATH}/${LOG_NAME}/logs_${BENCHMARK}_populatemetastore_${ENGINE}_${FILEFORMAT}_${SCALE_FACTOR}_`date +%Y%m%d%H%M%S`.log"
	SILENCE="2> /dev/null 1> /dev/null"
	if [ "X$DEBUG_SCRIPT" != "X" ]; then
        	SILENCE=""
	fi

	# Create the text/flat tables as external tables. These will be later be converted to ${FILEFORMAT}.
	start=$(date +%s%3N)
	DATE_PREFIX "INFO" "Loading text data into external tables." 2>&1 | tee $POPULATE_LOG
	COMMAND="${EXECUTION_BINARY}  -i ${POPULATE_SETTING} -f ${CURRENT_DIR}/ddl-${BENCHMARK}/text/alltables.sql -d DB=${BENCHMARK}_text_${SCALE_FACTOR} -d LOCATION=${RAW_DATA_DIR}/${SCALE_FACTOR}"
	DATE_PREFIX "INFO" "The command is: ${COMMAND}" 2>&1 | tee -a $POPULATE_LOG
	${COMMAND} 2>&1 | >> $POPULATE_LOG
	DATE_PREFIX "INFO" "Loading external text tables done!" 2>&1 | tee -a $POPULATE_LOG

	i=1
	MAX_REDUCERS=2500 # maximum number of useful reducers for any scale
	REDUCERS=$((test ${SCALE_FACTOR} -gt ${MAX_REDUCERS} && echo ${MAX_REDUCERS}) || echo ${SCALE_FACTOR})

	# Populate the smaller tables.
	DATE_PREFIX "INFO" "Start populating tables..." 2>&1 | tee -a $POPULATE_LOG
	
	for t in ${TABLES}
	do
        	COMMAND="${EXECUTION_BINARY}  -i ${POPULATE_SETTING} -f ${CURRENT_DIR}/ddl-${BENCHMARK}/bin_partitioned/${t}.sql -d DB=${DATABASE} -d SOURCE=${BENCHMARK}_text_${SCALE_FACTOR} -d SCALE=${SCALE_FACTOR} -d REDUCERS=${REDUCERS} -d FILE=${FILEFORMAT}"
		## add app name if the engine is sparksql
		if [[ "X${ENGINE}" = "Xsparksql" ]]; then
			COMMAND="${COMMAND} --name populate-${t}"
		fi
        	DATE_PREFIX "INFO" "($i/$TOTAL) Populating table: $t." 2>&1 | tee -a $POPULATE_LOG
		DATE_PREFIX "INFO" "The command is: ${COMMAND}" 2>&1 | tee -a $POPULATE_LOG
		$COMMAND 2>&1 | >> $POPULATE_LOG
		RES=${PIPESTATUS[0]}
		if [[ ${RES} == 0 ]]; then
			DATE_PREFIX "INFO" "Populating table: $t done!" 2>&1 | tee -a $POPULATE_LOG
		else
			DATE_PREFIX "ERROR" "Populating table: $t failed, please re-try later, exiting..." 2>&1 | tee -a $POPULATE_LOG
			exit -2
		fi
        	i=`expr $i + 1`
	done
	
	# Only do for TPC-DS
	if [ "X${BENCHMARK}" = "Xtpcds" ]; then
		for t in ${FACTS}
		do
	        	COMMAND="${EXECUTION_BINARY} -i ${POPULATE_SETTING} -f ${CURRENT_DIR}/ddl-${BENCHMARK}/bin_partitioned/${t}.sql -d DB=${DATABASE} -d SCALE=${SCALE_FACTOR} -d SOURCE=${BENCHMARK}_text_${SCALE_FACTOR} -d BUCKETS=${BUCKETS} -d RETURN_BUCKETS=${RETURN_BUCKETS} -d REDUCERS=${REDUCERS} -d FILE=${FILEFORMAT}"
			## add app name if the engine is sparksql
                	if [[ "X${ENGINE}" = "Xsparksql" ]]; then
                        	COMMAND="${COMMAND} --name populate-${t}"
                	fi
			DATE_PREFIX "INFO" "($i/$TOTAL) Populating table: $t." 2>&1 | tee -a $POPULATE_LOG
	                DATE_PREFIX "INFO" "The command is: ${COMMAND}" 2>&1 | tee -a $POPULATE_LOG
	                $COMMAND 2>&1 | >> $POPULATE_LOG
	                RES=${PIPESTATUS[0]}
	                if [[ ${RES} == 0 ]]; then
	                        DATE_PREFIX "INFO" "Populating table: $t done!" 2>&1 | tee -a $POPULATE_LOG
	                else
	                        DATE_PREFIX "ERROR" "Populating table: $t failed, please re-try later, exiting..." 2>&1 | tee -a $POPULATE_LOG
	                        exit -3
	                fi
	        	i=`expr $i + 1`
		done
	fi
	end=$(date +%s%3N)
        getExecTime $start $end 2>&1 | tee -a $POPULATE_LOG
        DATE_PREFIX "INFO" "Populating tables done!" 2>&1 | tee -a $POPULATE_LOG
	# analyze the tables
	DATE_PREFIX "INFO" "Analyzing tables..." 2>&1 | tee -a $POPULATE_LOG
	COMMAND="${EXECUTION_BINARY}  -i ${POPULATE_SETTING} -f ${ANALYZE_SQL} --database ${DATABASE}"
	DATE_PREFIX "INFO" "The command is: ${COMMAND}" 2>&1 | tee -a $POPULATE_LOG
	$COMMAND 2>&1 | >> $POPULATE_LOG
	RES=${PIPESTATUS[0]}
	if [[ ${RES} == 0 ]]; then
		DATE_PREFIX "INFO" "Analyzing tables done!" 2>&1 | tee -a $POPULATE_LOG
	else
		DATE_PREFIX "ERROR" "Analyzing tables failed!" 2>&1 | tee -a $POPULATE_LOG
		exit -4
	fi
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
	
	LOCAL_SETTING="${LOCAL_SETTING_ROOT}/${QUERY_NAME}.sql"
	QUERY_NAME="query${1}"
	if [ ${ENGINE} = "sparksql" ]; then
		QUERY_NAME="q${1}"
		LOCAL_SETTING="${LOCAL_SETTING_ROOT}/${QUERY_NAME}.conf"
	fi
	OPTION=()
	
	if [[ ${ENGINE} == "mr" || ${ENGINE} == "spark" ]]; then
		OPTION=(-i ${BENCH_SETTING})
        	if [ -e ${GLOBAL_SETTING} ]; then
                	OPTION+=(-i ${GLOBAL_SETTING})
        	fi
        	if [ -e ${LOCAL_SETTING} ]; then
                	OPTION+=(-i ${LOCAL_SETTING})
        	fi
		OPTION+=(-f ${QUERY_ROOT}/${QUERY_NAME}.sql --database ${DATABASE})
		## keep print setting at last
        	if [ -e ${PRINT_SETTING} ]; then
                	OPTION+=(-i ${PRINT_SETTING})
        	fi
		CMD="hive ${OPTION[@]}"
	elif [[ ${ENGINE} == "sparksql" ]]; then
		if [ -e ${LOCAL_SETTING} ]; then
			DATE_PREFIX "INFO" "Local setting file exist for query${1}, will merge benchmark setting and SparkSQL engine settings to this file..."	
                	Merge_conf ${LOCAL_SETTING} ${BENCH_SETTING} ${SPARKSQL_USER_CONF} ${LOCAL_SETTING} 
			DATE_PREFIX "INFO" "Merging configuration file done!"
		else
			DATE_PREFIX "INFO" "Local setting file does not exist, will automatically generate it using benchmark setting and SparkSQL engine settings..."
			Merge_conf ${BENCH_SETTING} ${SPARKSQL_USER_CONF} ${LOCAL_SETTING}
			DATE_PREFIX "INFO" "Automatically generating configuration file done!"
        	fi
		
		OPTION+=(--properties-file ${LOCAL_SETTING} -f ${QUERY_ROOT}/${QUERY_NAME}.sql --database ${DATABASE} --name ${QUERY_NAME})
		CMD="${SPARK_HOME}/bin/spark-sql ${OPTION[@]}"
	else
		DATE_PREFIX "ERROR" "Currently only support engine: mr/spark/sparksql, exiting..."
		exit -5
	fi
	
	QUERY_LOG=${OUT_DIR_PATH}/${LOG_NAME}/${QUERY_NAME}.log	
	DATE_PREFIX "INFO" "Running query$1 with command: ${CMD}" 2>&1 | tee ${QUERY_LOG}
	start=$(date +%s%3N)
	${CMD} 2>&1 | tee -a ${QUERY_LOG}
	RES=${PIPESTATUS[0]}
	end=$(date +%s%3N)
	getExecTime $start $end 2>&1 | tee -a ${QUERY_LOG}
	if [[ ${RES} == 0 ]]; then
		DATE_PREFIX "INFO" "query$1 finished successfully!" 2>&1 | tee -a ${QUERY_LOG}
	else
		DATE_PREFIX "ERROR" "query$1 failed!" 2>&1 | tee -a ${QUERY_LOG}
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
	if [ -e ${POPULATE_SETTING} ]; then
                cp -r ${POPULATE_SETTING} ${SETTING_DIR}
        fi
}

## run all the queries defined in ${QUERY_LIST} one by one
function runAll(){
	if [[ $# != 1 ]]; then
                DATE_PREFIX "INFO" "Usage: runAll rounds_to_run"
		exit 2
        fi

	for ((r=1; r<=$1; r++))
	do
		clearCache
		DATE_PREFIX "INFO" "Running round $r"
		export LOG_NAME=logs_${BENCHMARK}_${ENGINE}_${FILEFORMAT}_${SCALE_FACTOR}_`date +%Y%m%d%H%M%S`
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
	if [ ${CACHE_CLEAR} = "true" ] ; then
		if [ ! -z ${HOSTS} ]; then
			pssh -H ${HOSTS} -t 0  -i "sync; echo 3 > /proc/sys/vm/drop_caches && printf '\n%s\n' 'Ram-cache Cleared'"
			pssh -H ${HOSTS} -t 0  -i free -g
		else
			DATE_PREFIX "WARN" "Clear cache is chosen but hosts file does not exists, will not clear cache!"
		fi
	else
		DATE_PREFIX "INFO" "Will not automatically clear cache!"
	fi
}

################################################################################
## Start From Here!
################################################################################
#dataGen
#populateMetastore
#runAll 3
#runQuery 24b
