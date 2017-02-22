#!/bin/bash

if [ $# = 0 ]; then
    yesterday=`date +%Y%m%d -d "-1days"`
else
    yesterday=$1
fi

export JAVA_HOME=${JAVA_HOME}
export HADOOP_HOME=${HADOOP_HOME}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}
baseDir=$(cd "$(dirname "$0")"; pwd)
source $baseDir/../weblogHeader.sh

echo $yesterday

zyStatus="fault"
zyHours=0
zyIp=0


function check(){
	zyHours=`${HADOOP} fs -du ${ZYLOG_DIR}/$yesterday/ | grep $yesterday |awk -F'/' '{print $5}'| awk -F'_' '{print $3}'  |sort -u |wc -l`

	zyIp=`${HADOOP} fs -du ${ZYLOG_DIR}/$yesterday/ | grep $yesterday |awk -F'/' '{print $5}'| awk -F'_' '{print $4}'  |sort -u |wc -l`
	if [ $zyHours -eq 24 -a $zyIp -eq 6 ];then

	    zyStatus="success"
	fi
}
function move(){
	devilfishLog_wap_HadoopDestDir=${WEBLOG_COMMMON_HDFS_DIR}devilfishLogClassification/wap
	hadoopDirPrepare $devilfishLog_wap_HadoopDestDir $yesterday
	${HADOOP} fs -mv ${ZYLOG_DIR}/$yesterday/wap_* ${WEBLOG_COMMMON_HDFS_DIR}devilfishLogClassification/wap/$yesterday/
}

function devilfishLogClassification(){
	devilfishLog_www_HadoopDestDir=${WEBLOG_COMMMON_HDFS_DIR}devilfishLogClassification/devilfishLog_www
	shareLog_www_HadoopDestDir=${WEBLOG_COMMMON_HDFS_DIR}devilfishLogClassification/shareLog_www
	shareBackLog_sps_HadoopDestDir=${WEBLOG_COMMMON_HDFS_DIR}devilfishLogClassification/shareBackLog_sps

	hadoopDirPrepare $devilfishLog_www_HadoopDestDir $yesterday
	${HADOOP} fs -cp ${ZYLOG_DIR}/$yesterday/* $devilfishLog_www_HadoopDestDir/$yesterday/
	
	hadoopDirPrepare $shareLog_www_HadoopDestDir $yesterday
	${HADOOP} fs -mv $devilfishLog_www_HadoopDestDir/$yesterday/snstj_*  $shareLog_www_HadoopDestDir/$yesterday/
	
	hadoopDirPrepare $shareBackLog_sps_HadoopDestDir $yesterday
	${HADOOP} fs -mv $devilfishLog_www_HadoopDestDir/$yesterday/sps_*  $shareBackLog_sps_HadoopDestDir/$yesterday/
}
function main(){
	
	check	
	if [ "$zyStatus" == "fault" ];then
		errorAlarm zyCheck:zyHours=$zyHours/24
		exit 1
	else
	    move
	    devilfishLogClassification
		exit 0
	fi
}

main
