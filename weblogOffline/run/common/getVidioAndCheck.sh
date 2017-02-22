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
vidioLocalDestDir=${WEBLOG_RESULT_LOCAL_DIR}weblogRsync/vidio/$yesterday
vidioHadoopDestDir=${WEBLOG_COMMMON_HDFS_DIR}vidioIncr
vidioStatus="fault"
vidioFileNum=1
vidioRealFileNum=0
#最大重试次数,设为0则不重试
maxRetry=10
retry=0
#重试间隔(s)
interval=180
#yc
function getAndCheckVidio(){
   if [  -d $vidioLocalDestDir ];then
        rm  $vidioLocalDestDir/*
	else
	    mkdir -p $vidioLocalDestDir
	fi
	
	vidioLocalFile=$vidioLocalDestDir/${yesterday}.vidio
	sh $baseDir/../runCommand.sh com.netease.weblogOffline.common.getVidioToLacol $yesterday $vidioLocalFile
  	hadoopDirPrepare $vidioHadoopDestDir $yesterday
    ${HADOOP} fs -put $vidioLocalDestDir/* $vidioHadoopDestDir/$yesterday/

	vidioRealFileNum=`${HADOOP} fs -du $vidioHadoopDestDir/$yesterday/ | grep $yesterday -c`
	if [ $vidioRealFileNum -ge $vidioFileNum ];then
	    vidioStatus="success"
	fi
}


function prepareVidiolog(){
	while [ "$vidioStatus" == "fault" -a $retry -le $maxRetry ];do
        getAndCheckVidio
		if [ "$vidioStatus" == "fault" ];then
			retry=$((retry+1))
			echo getvidioLog sleep, retry=$retry/$maxRetry fileCount=$vidioRealFileNum/$vidioFileNum
			sleep $interval"s"
		fi
	done
}

function main(){  
prepareVidiolog	
    if [ "$vidioStatus" == "fault" ];then
    	errorAlarm getvidioLog:vidioFileCount=$vidioRealFileNum/$vidioFileNum
		exit 1
	else
		exit 0
	fi
}

main