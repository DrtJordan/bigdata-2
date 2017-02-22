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
gips="220.181.98.191"
gLocalDestDir=${WEBLOG_RESULT_LOCAL_DIR}weblogRsync/3g/$yesterday
gHadoopDestDir=${WEBLOG_COMMMON_HDFS_DIR}contentIncr_3g
gStatus="fault"
gfileNum=1
gRealFileNum=0
#最大重试次数,设为0则不重试
maxRetry=10
retry=0
#重试间隔(s)
interval=180
#yc
function getAndCheck3g(){
   if [  -d $gLocalDestDir ];then
        rm  $gLocalDestDir/*
	else
	    mkdir -p $gLocalDestDir
	fi
	for gip in $gips;do
		echo get3g from $gip	
		glocalFile=$gLocalDestDir/${yesterday}.${gip}.3g
		/usr/bin/rsync -au root@$gip::newsclient/NewArticleStatistic$yesterday $glocalFile
	done
  	hadoopDirPrepare $gHadoopDestDir $yesterday
    ${HADOOP} fs -put $gLocalDestDir/* $gHadoopDestDir/$yesterday/

	gRealFileNum=`${HADOOP} fs -du $gHadoopDestDir/$yesterday/ | grep $yesterday -c`
	if [ $gRealFileNum -ge $gfileNum ];then
	    gStatus="success"
	fi
}


function prepare3glog(){
	while [ "$gStatus" == "fault" -a $retry -le $maxRetry ];do
        getAndCheck3g
		if [ "$gStatus" == "fault" ];then
			retry=$((retry+1))
			echo get3glog sleep, retry=$retry/$maxRetry fileCount=$gRealFileNum/$gfileNum
			sleep $interval"s"
		fi
	done
}

function main(){  
prepare3glog	
    if [ "$gStatus" == "fault" ];then
    	errorAlarm get3glog:3gFileCount=$gRealFileNum/$gfileNum
		exit 1
	else
		exit 0
	fi
}

main