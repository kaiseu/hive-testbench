SCALE=1000
DIR=/tmp/tpch
TABLET_SERVER='skl-slave9'
time=`date +%Y%m%d%H%M%S`
CURRENT_DIR=$( cd $( dirname ${BASH_SOURCE[0]} ) && pwd )
LOG_DIR=${CURRENT_DIR}/output/logs_tpch_impala_${SCALE}_${time}
if [ ! -d ${LOG_DIR} ]; then
	mkdir -p ${LOG_DIR}
fi

IMPALA_DB_NAME=tpch_impala_${SCALE}
KUDU_DB_NAME=tpch_kudu_${SCALE}

function getExecTime() {
        start=$1
        end=$2
        time_s=`echo "scale=3;$(($end-$start))/1000" | bc`
        echo "Duration: ${time_s} s"
}

function Load(){
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

function runQuery(){
	query=$1
	echo "`date` run query ${query}..."  2>&1 | tee ${LOG_DIR}/tpch_query${query}.log
	CMD="impala-shell -i ${TABLET_SERVER} -f sample-queries-tpch-impala/tpch_query${query}.sql -d ${KUDU_DB_NAME}"
	echo ${CMD} 2>&1 | tee -a ${LOG_DIR}/tpch_query${query}.log
	start=$(date +%s%3N)
	eval ${CMD} 2>&1 | tee -a ${LOG_DIR}/tpch_query${query}.log
	end=$(date +%s%3N)
	getExecTime $start $end >> ${LOG_DIR}/tpch_query${query}.log
	echo "`date` query ${query} done!" >> ${LOG_DIR}/tpch_query${query}.log
}

function runAll(){
	for n in {1..22}; do
		runQuery ${n}
	done
}

function cleanCache(){
	pssh -h /root/slaves -t 0  -i "sync; echo 3 > /proc/sys/vm/drop_caches && printf '\n%s\n' 'Ram-cache Cleared'"
}

#Load
#cleanCache
runAll
