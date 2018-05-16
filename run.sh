SCALE_FACTOR="1000"
ENGINE="mr"
OUT_DIR_PATH="output"
LOG_NAME="logs"
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
	
	OPTION=(-i sample-queries-tpch/testbench.settings) 
	LOCAL_SETTING="./sample-queries-tpch/conf/tpch_query${1}_${ENGINE}.settings"
	if [ -e ${LOCAL_SETTING} ]; then
		OPTION+=(-i ${LOCAL_SETTING})
	fi
	
	OPTION+=(-f sample-queries-tpch/tpch_query${1}.sql --database ${DATABASE})
	PRINT_SETTING="./sample-queries-tpch/print.settings"
	if [ -e ${PRINT_SETTING} ]; then
		 OPTION+=(-i ${PRINT_SETTING})
	fi

	CMD="hive ${OPTION[@]}"
	echo "Running query$1 with command: ${CMD}"
	start=$(date +%s%3N)
	hive ${OPTION[@]} 2>&1 | tee ${OUT_DIR_PATH}/${LOG_NAME}/tpch_query${1}.log 
	end=$(date +%s%3N)
	getExecTime $start $end >> ${OUT_DIR_PATH}/${LOG_NAME}/tpch_query${1}.log
	echo "query$1 finished!"
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
		export LOG_NAME=logs_${SCALE_FACTOR}_`date +%Y%m%d%H%M%S`
		for q in {1..18};
		do
			runQuery $q
		done
		
		for q in {20..22};do runQuery $q; done
		echo "Round $r finished, logs are saved into: ${OUT_DIR_PATH}/${LOG_NAME}"
	done
}

#runAll 2
runQuery 21
