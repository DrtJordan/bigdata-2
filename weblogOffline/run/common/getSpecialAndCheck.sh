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
specialIps="61.135.251.68"
specialLocalDestDir=${WEBLOG_RESULT_LOCAL_DIR}weblogRsync/special/$yesterday
specialHadoopDestDir=${WEBLOG_COMMMON_HDFS_DIR}specialIncr
specialStatus="fault"
specialFileNum=1
specialRealFileNum=0
#最大重试次数,设为0则不重试
maxRetry=10
retry=0
#重试间隔(s)
interval=180
#yc
function getAndCheckSpecial(){
   if [  -d $specialLocalDestDir ];then
        rm  $specialLocalDestDir/*
	else
	    mkdir -p $specialLocalDestDir
	fi
	for specialIp in $specialIps;do
		echo getSpecial from $specialIp	
		specialLocalFile=$specialLocalDestDir/${yesterday}.${specialIp}.special
		/usr/bin/rsync -au root@$specialIp::articlemodify/${yesterday}_special* $specialLocalFile
	done
  	hadoopDirPrepare $specialHadoopDestDir $yesterday
    ${HADOOP} fs -put $specialLocalDestDir/* $specialHadoopDestDir/$yesterday/

	specialRealFileNum=`${HADOOP} fs -du $specialHadoopDestDir/$yesterday/ | grep $yesterday -c`
	if [ $specialRealFileNum -ge $specialFileNum ];then
	    specialStatus="success"
	fi
}


function prepareSpeciallog(){
	while [ "$specialStatus" == "fault" -a $retry -le $maxRetry ];do
        getAndCheckSpecial
		if [ "$specialStatus" == "fault" ];then
			retry=$((retry+1))
			echo getSpecialLog sleep, retry=$retry/$maxRetry fileCount=$specialRealFileNum/$specialFileNum
			sleep $interval"s"
		fi
	done
}

function main(){  
prepareSpeciallog	
    if [ "$specialStatus" == "fault" ];then
    	errorAlarm getSpecialLog:specialFileCount=$specialRealFileNum/$specialFileNum
		exit 1
	else
		exit 0
	fi
}

main