SCALE=1000
DIR=/tmp/tpch
TABLET_SERVER='skl-slave9'
FORMAT='TEXTFILE'
PARTITION=true
CURRENT_DIR=$( cd $( dirname ${BASH_SOURCE[0]} ) && pwd )
IMPALA_CONF_FILE=${CURRENT_DIR}/sample-queries-tpch-impala/conf/impala.conf
LOG_DIR="output/"
IMPALA_DB_NAME=tpch_impala_${SCALE}
if [[ ${PARTITION}==true ]]; then {
	PARTITION_DB_NAME=tpch_impala_${SCALE}_partition
	KUDU_DB_NAME=tpch_kudu_${SCALE}_partition
} else {
	KUDU_DB_NAME=tpch_kudu_${SCALE}
}
fi

function mkLogDir(){
time=`date +%Y%m%d%H%M%S`
export LOG_DIR=${CURRENT_DIR}/output/logs_tpch_impala_${SCALE}_${time}
export IMPALA_RESULT_FILE=${CURRENT_DIR}/output/impala_result.log
if [ ! -d ${LOG_DIR} ]; then
        mkdir -p ${LOG_DIR}
fi
if [ ! -f ${IMPALA_RESULT_FILE} ]; then
        touch ${IMPALA_RESULT_FILE}
fi
}

function getExecTime() {
        start=$1
        end=$2
        time_s=`echo "scale=3;$(($end-$start))/1000" | bc`
        echo "Duration: ${time_s} s"
}

function Load_Flat(){
	hadoop fs -chmod -R 777 ${DIR}/${SCALE}
	echo "`date` Loading text data into external tables." 2>&1 | tee ${LOG_DIR}/load_alltables.log
	impala-shell -i ${TABLET_SERVER} -q "create database if not exists ${IMPALA_DB_NAME}" 2>&1 | tee -a ${LOG_DIR}/load_alltables.log
	impala-shell -i ${TABLET_SERVER} -q "create database if not exists ${KUDU_DB_NAME}" 2>&1 | tee -a ${LOG_DIR}/load_alltables.log
	start=$(date +%s%3N)
	impala-shell -i ${TABLET_SERVER} -f ddl-tpch/bin_flat_kudu/alltables.sql -d ${IMPALA_DB_NAME} --var=DB=${IMPALA_DB_NAME} --var=LOCATION=${DIR}/${SCALE} --var=KUDU_DB_NAME=${KUDU_DB_NAME} 2>&1 | tee -a ${LOG_DIR}/load_alltables.log
	end=$(date +%s%3N)
	getExecTime $start $end >> ${LOG_DIR}/load_alltables.log
	echo "`date` Loading done!" >> ${LOG_DIR}/load_alltables.log

	echo "`date` Starting compute stats for tables..." 2>&1 | tee -a ${LOG_DIR}/load_alltables.log
	start=$(date +%s%3N)
	impala-shell -i ${TABLET_SERVER} -f ddl-tpch/bin_flat_kudu/computeStats.sql -d ${KUDU_DB_NAME} --var=DB=${KUDU_DB_NAME} 2>&1 | tee -a ${LOG_DIR}/load_alltables.log
	end=$(date +%s%3N)
	getExecTime $start $end >> ${LOG_DIR}/load_alltables.log
        echo "`date` Compute done!" >> ${LOG_DIR}/load_alltables.log
}

function Load_Partition(){
        hadoop fs -chmod -R 777 ${DIR}/${SCALE}
	LOG_FILE_NAME="load_alltables_partition.log"
        start=$(date +%s%3N)
        echo "`date` Loading text data into external tables." 2>&1 | tee ${LOG_DIR}/${LOG_FILE_NAME}
        impala-shell -i ${TABLET_SERVER} -q "create database if not exists ${IMPALA_DB_NAME}" 2>&1 | tee -a ${LOG_DIR}/${LOG_FILE_NAME}
        impala-shell -i ${TABLET_SERVER} -f ddl-tpch/bin_partitioned_kudu/alltables_source.sql -d ${IMPALA_DB_NAME} --var=SOURCE_DB=${IMPALA_DB_NAME} --var=LOCATION=${DIR}/${SCALE} --var=FORMAT=${FORMAT} 2>&1 | tee -a ${LOG_DIR}/${LOG_FILE_NAME}

        echo "`date` Loading partitioned tables." 2>&1 | tee -a ${LOG_DIR}/${LOG_FILE_NAME}
        impala-shell -i ${TABLET_SERVER} -q "create database if not exists ${PARTITION_DB_NAME}" 2>&1 | tee -a ${LOG_DIR}/${LOG_FILE_NAME}
        impala-shell -i ${TABLET_SERVER} -f ddl-tpch/bin_partitioned_kudu/alltables_partition.sql -d ${PARTITION_DB_NAME} --var=SOURCE_DB=${IMPALA_DB_NAME} --var=FORMAT=${FORMAT} 2>&1 | tee -a ${LOG_DIR}/${LOG_FILE_NAME}

        echo "`date` Loading kudu tables." 2>&1 | tee -a ${LOG_DIR}/${LOG_FILE_NAME}
        impala-shell -i ${TABLET_SERVER} -q "create database if not exists ${KUDU_DB_NAME}" 2>&1 | tee -a ${LOG_DIR}/${LOG_FILE_NAME}
        impala-shell -i ${TABLET_SERVER} -f ddl-tpch/bin_partitioned_kudu/alltables_kudu.sql -d ${KUDU_DB_NAME} --var=SOURCE_DB=${IMPALA_DB_NAME} --var=KUDU_DB_NAME=${KUDU_DB_NAME} 2>&1 | tee -a ${LOG_DIR}/${LOG_FILE_NAME}
        end=$(date +%s%3N)
        getExecTime $start $end >> ${LOG_DIR}/${LOG_FILE_NAME}
        echo "`date` Loading done!" >> ${LOG_DIR}/${LOG_FILE_NAME}

        echo "`date` Starting compute stats for tables..." 2>&1 | tee -a ${LOG_DIR}/${LOG_FILE_NAME}
        start=$(date +%s%3N)
        impala-shell -i ${TABLET_SERVER} -f ddl-tpch/bin_partitioned_kudu/computeStats.sql -d ${KUDU_DB_NAME} --var=DB=${KUDU_DB_NAME} 2>&1 | tee -a ${LOG_DIR}/${LOG_FILE_NAME}
        end=$(date +%s%3N)
        getExecTime $start $end >> ${LOG_DIR}/${LOG_FILE_NAME}
        echo "`date` Compute done!" >> ${LOG_DIR}/${LOG_FILE_NAME}
}

function Load() {
if [[ ${PARTITION}==true ]]; then 
	Load_Partition
else 
	Load_Flat

fi
}

function runQuery(){
	query=$1
	echo "`date` run query ${query}..."  2>&1 | tee ${LOG_DIR}/tpch_query${query}.log
	CMD="impala-shell -i ${TABLET_SERVER} -p -f sample-queries-tpch-impala/tpch_query${query}.sql -d ${KUDU_DB_NAME} -o ${IMPALA_RESULT_FILE}"
	if [[ -f ${IMPALA_CONF_FILE} ]]; then
		CMD+=" --config_file=${IMPALA_CONF_FILE}"
	fi
	echo ${CMD} 2>&1 | tee -a ${LOG_DIR}/tpch_query${query}.log
	start=$(date +%s%3N)
	eval ${CMD} 2>&1 | tee -a ${LOG_DIR}/tpch_query${query}.log
	end=$(date +%s%3N)
	getExecTime $start $end >> ${LOG_DIR}/tpch_query${query}.log
	echo "`date` query ${query} done!" >> ${LOG_DIR}/tpch_query${query}.log
}

function runAll(){
	mkLogDir
	for n in {1..22}; do
		runQuery ${n}
	done
}

function cleanCache(){
	pssh -h /root/slaves -t 0  -i "sync; echo 3 > /proc/sys/vm/drop_caches && printf '\n%s\n' 'Ram-cache Cleared'"
	pssh -h /root/slaves -t 0  -i free -g
}

#Load
cleanCache
runAll
sleep 120

