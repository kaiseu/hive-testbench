SCALE_FACTOR="1000"
## engine to run, can be mr spark sparksql
ENGINE="sparksql"

LOG_NAME="logs"
CURRENT_DIR=$( cd $( dirname ${BASH_SOURCE[0]} ) && pwd )
BENCH_SETTING="${CURRENT_DIR}/sample-queries-tpch/testbench.settings"
GLOBAL_SETTING="${CURRENT_DIR}/sample-queries-tpch/conf/${ENGINE}.settings"
LOCAL_SETTING_ROOT="${CURRENT_DIR}/sample-queries-tpch/conf/${ENGINE}"
PRINT_SETTING="${CURRENT_DIR}/sample-queries-tpch/conf/print.settings"
SPARK_USER_CONF="${CURRENT_DIR}/sample-queries-tpch/conf/spark.conf"
QUERY_ROOT="${CURRENT_DIR}/sample-queries-tpch"
OUT_DIR_PATH="${CURRENT_DIR}/output"


if test ${SCALE_FACTOR} -le 1000; then
	SCHEMA_TYPE=flat
else
	SCHEMA_TYPE=partitioned
fi
DATABASE=tpch_${SCHEMA_TYPE}_orc_${SCALE_FACTOR}

if [[ ! -d ${OUT_DIR_PATH}/${LOG_NAME} ]];then
	echo "Creating output dir: ${OUT_DIR_PATH}/${LOG_NAME}"
	mkdir -p ${OUT_DIR_PATH}/${LOG_NAME}
	echo "Dir Created!"
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
function getExecTime() {
	start=$1
	end=$2
	time_s=`echo "scale=3;$(($end-$start))/1000" | bc`
	echo "Duration: ${time_s} s"
}

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

	LOCAL_SETTING="${CURRENT_DIR}/sample-queries-tpch/conf/${ENGINE}/tpch_query${1}_${ENGINE}.settings"
#	if [ -e ${LOCAL_SETTING} ]; then
#		OPTION+=(-i ${LOCAL_SETTING})
#	fi
	
#	OPTION+=(-f ${QUERY_ROOT}/tpch_query${1}.sql --database ${DATABASE})
	
	## keep print setting at last
       # if [ -e ${PRINT_SETTING} ]; then
       #          OPTION+=(-i ${PRINT_SETTING})
       # fi
	
	if [[ ${ENGINE} == "mr" || ${ENGINE} == "spark" ]]; then
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
		if [ -e ${PRINT_SETTING} ]; then
                	OPTION+=(--properties-file ${SPARK_USER_CONF})
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
		echo "Currently only support engine: mr/spark/sparksql, exiting..."
		exit -1
	fi
	
	echo "Running query$1 with command: ${CMD}" 2>&1 | tee ${OUT_DIR_PATH}/${LOG_NAME}/tpch_query${1}.log
	start=$(date +%s%3N)
	eval ${CMD} 2>&1 | tee -a ${OUT_DIR_PATH}/${LOG_NAME}/tpch_query${1}.log
	RES=$?
	end=$(date +%s%3N)
	getExecTime $start $end >> ${OUT_DIR_PATH}/${LOG_NAME}/tpch_query${1}.log
	if [[ ${RES} == 0 ]]; then
		echo "query$1 finished successfully!" >> ${OUT_DIR_PATH}/${LOG_NAME}/tpch_query${1}.log
	else
		echo "query$1 failed!" >> ${OUT_DIR_PATH}/${LOG_NAME}/tpch_query${1}.log
	fi
}

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
	if [ -e ${SPARK_USER_CONF} ]; then
                cp -r ${SPARK_USER_CONF} ${SETTING_DIR}
        fi
}

function runAll(){
	if [[ $# != 1 ]]; then
                echo "Usage: runAll rounds_to_run"
		exit 2
        fi

	for ((r=1; r<=$1; r++))
	do
		/mnt/PAT/clear_cache.sh
		echo "Running round $r"
		export LOG_NAME=logs_${ENGINE}_${SCALE_FACTOR}_`date +%Y%m%d%H%M%S`
		for q in {1..22};
		do
			runQuery $q
		done
		
		#for q in {20..22};do runQuery $q; done
		echo "Round $r finished, logs are saved into: ${OUT_DIR_PATH}/${LOG_NAME}"
		## Backup the corresponding settings to log dir
		BACKUP ${OUT_DIR_PATH}/${LOG_NAME}
	done
}

#runAll 2
runQuery 15
#runQuery 2
