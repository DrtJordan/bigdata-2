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
dyDataIps="10.120.146.185"
dyDataLocalDestDir=${WEBLOG_RESULT_LOCAL_DIR}weblogRsync/dyData/$yesterday
dyDataHadoopDestDir=${WEBLOG_COMMMON_HDFS_DIR}dyDataIncr
dyDataStatus="fault"
dyDataFileNum=1
dyDataRealFileNum=0
#最大重试次数,设为0则不重试
maxRetry=10
retry=0
#重试间隔(s)
interval=180
#dyData
function getAndCheckDyData(){
   if [  -d $dyDataLocalDestDir ];then
        rm  $dyDataLocalDestDir/*
	else
	    mkdir -p $dyDataLocalDestDir
	fi
	for dyDataIp in $dyDataIps;do
		echo getdyData from $dyDataIp	
		dyDataLocalFile=$dyDataLocalDestDir/${yesterday}.${dyDataIp}.dyData
		/usr/bin/rsync -avzP root@$dyDataIp::statistic/SubscribeArticleStatistic${yesterday} $dyDataLocalFile
	done
  	hadoopDirPrepare $dyDataHadoopDestDir $yesterday
    ${HADOOP} fs -put $dyDataLocalDestDir/* $dyDataHadoopDestDir/$yesterday/

	dyDataRealFileNum=`${HADOOP} fs -du $dyDataHadoopDestDir/$yesterday/ | grep $yesterday -c`
	if [ $dyDataRealFileNum -ge $dyDataFileNum ];then
	    dyDataStatus="success"
	fi
}


function prepareDyDatalog(){
	while [ "$dyDataStatus" == "fault" -a $retry -le $maxRetry ];do
        getAndCheckDyData
		if [ "$dyDataStatus" == "fault" ];then
			retry=$((retry+1))
			echo getdyDataLog sleep, retry=$retry/$maxRetry fileCount=$dyDataRealFileNum/$dyDataFileNum
			sleep $interval"s"
		fi
	done
}

function main(){  
    prepareDyDatalog	
    if [ "$dyDataStatus" == "fault" ];then
    	errorAlarm getdyDataLog:dyDataFileCount=$dyDataRealFileNum/$dyDataFileNum
		exit 1
	else
		exit 0
	fi
}

main