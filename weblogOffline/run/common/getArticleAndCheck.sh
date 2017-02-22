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
articleIps="61.135.251.68"
articleLocalDestDir=${WEBLOG_RESULT_LOCAL_DIR}weblogRsync/article/$yesterday
articleHadoopDestDir=${WEBLOG_COMMMON_HDFS_DIR}articleIncr
articleStatus="fault"
articleFileNum=1
articleRealFileNum=0
#最大重试次数,设为0则不重试
maxRetry=10
retry=0
#重试间隔(s)
interval=180
#article
function getAndCheckArticle(){
   if [  -d $articleLocalDestDir ];then
        rm  $articleLocalDestDir/*
	else
	    mkdir -p $articleLocalDestDir
	fi
	for articleIp in $articleIps;do
		echo getArticle from $articleIp	
		articleLocalFile=$articleLocalDestDir/${yesterday}.${articleIp}.article
		/usr/bin/rsync -au root@$articleIp::articlemodify/${yesterday}_article* $articleLocalFile
	done
  	hadoopDirPrepare $articleHadoopDestDir $yesterday
    ${HADOOP} fs -put $articleLocalDestDir/* $articleHadoopDestDir/$yesterday/

	articleRealFileNum=`${HADOOP} fs -du $articleHadoopDestDir/$yesterday/ | grep $yesterday -c`
	if [ $articleRealFileNum -ge $articleFileNum ];then
	    articleStatus="success"
	fi
}


function prepareArticlelog(){
	while [ "$articleStatus" == "fault" -a $retry -le $maxRetry ];do
        getAndCheckArticle
		if [ "$articleStatus" == "fault" ];then
			retry=$((retry+1))
			echo getArticleLog sleep, retry=$retry/$maxRetry fileCount=$articleRealFileNum/$articleFileNum
			sleep $interval"s"
		fi
	done
}

function main(){  
    prepareArticlelog	
    if [ "$articleStatus" == "fault" ];then
    	errorAlarm getArticleLog:articleFileCount=$articleRealFileNum/$articleFileNum
		exit 1
	else
		exit 0
	fi
}

main