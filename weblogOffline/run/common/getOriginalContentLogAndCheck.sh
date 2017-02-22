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

ycLocalDestDir=${WEBLOG_RESULT_LOCAL_DIR}weblogRsync/ycInfo/$yesterday
ycHadoopDestDir=${WEBLOG_COMMMON_HDFS_DIR}originalContentIncr


ycInfoStatus="fault"
ycfileNum=3
ycRealFileNum=0

#最大重试次数,设为0则不重试
maxRetry=10
retry=0
#重试间隔(s)
interval=180

#yc
function getAndCheckYcInfo(){

    
    if [  -d $ycLocalDestDir ];then
        rm  $ycLocalDestDir/*
	else 
		mkdir -p $ycLocalDestDir
	fi
	/usr/bin/rsync -au 61.135.251.68::original/${yesterday}* $ycLocalDestDir/
	/usr/bin/rsync -au 220.181.29.156::originalPhotoset/${yesterday}* $ycLocalDestDir/ 
	
    hadoopDirPrepare $ycHadoopDestDir $yesterday
	${HADOOP} fs -put $ycLocalDestDir/$yesterday* $ycHadoopDestDir/$yesterday/
	
	ycRealFileNum=`${HADOOP} fs -du $ycHadoopDestDir/$yesterday/ | grep $yesterday -c`
	if [ $ycRealFileNum -ge $ycfileNum ];then
	    ycInfoStatus="success"
	fi

}


function prepareYclog(){


	while [ "$ycInfoStatus" == "fault" -a $retry -le $maxRetry ];do
        
        getAndCheckYcInfo
		if [ "$ycInfoStatus" == "fault" ];then
			retry=$((retry+1))
			echo getYclog sleep, retry=$retry/$maxRetry fileCount=$ycRealFileNum/$ycfileNum
			sleep $interval"s"
		fi
	done
	

}

function main(){  
prepareYclog	

    if [ "$ycInfoStatus" == "fault" ];then
        errorAlarm getYclog:YclogFileCount=$ycRealFileNum/$ycfileNum
		exit 1
	else
		exit 0
	fi
}

main

