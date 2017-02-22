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
photoSetIps="220.181.29.156"
photoSetLocalDestDir=${WEBLOG_RESULT_LOCAL_DIR}weblogRsync/photoSet/$yesterday
photoSetHadoopDestDir=${WEBLOG_COMMMON_HDFS_DIR}photoSetIncr
photoSetStatus="fault"
photoSetFileNum=1
photoSetRealFileNum=0
#最大重试次数,设为0则不重试
maxRetry=10
retry=0
#重试间隔(s)
interval=180
#yc
function getAndCheckPhotoSet(){
   if [  -d $photoSetLocalDestDir ];then
        rm  $photoSetLocalDestDir/*
	else
	    mkdir -p $photoSetLocalDestDir
	fi
	for PhotoSetIp in $photoSetIps;do
		echo getPhotoSet from $PhotoSetIp	
		photoSetLocalFile=$photoSetLocalDestDir/${yesterday}.${PhotoSetIp}.photoSet
		/usr/bin/rsync -au root@$PhotoSetIp::allPhotoset/${yesterday}* $photoSetLocalFile
	done
  	hadoopDirPrepare $photoSetHadoopDestDir $yesterday
    ${HADOOP} fs -put $photoSetLocalDestDir/* $photoSetHadoopDestDir/$yesterday/

	photoSetRealFileNum=`${HADOOP} fs -du $photoSetHadoopDestDir/$yesterday/ | grep $yesterday -c`
	if [ $photoSetRealFileNum -ge $photoSetFileNum ];then
	    photoSetStatus="success"
	fi
}


function preparePhotoSetlog(){
	while [ "$photoSetStatus" == "fault" -a $retry -le $maxRetry ];do
        getAndCheckPhotoSet
		if [ "$photoSetStatus" == "fault" ];then
			retry=$((retry+1))
			echo getPhotoSetLog sleep, retry=$retry/$maxRetry fileCount=$photoSetRealFileNum/$photoSetFileNum
			sleep $interval"s"
		fi
	done
}

function main(){  
preparePhotoSetlog	
    if [ "$photoSetStatus" == "fault" ];then
    	errorAlarm getPhotoSetLog:photoSetFileCount=$photoSetRealFileNum/$photoSetFileNum
		exit 1
	else
		exit 0
	fi
}

main