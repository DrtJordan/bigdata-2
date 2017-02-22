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
editorIps="220.181.98.191"
editorLocalDestDir=${WEBLOG_RESULT_LOCAL_DIR}weblogRsync/editor_3g/$yesterday
editorHadoopDestDir=${WEBLOG_COMMMON_HDFS_DIR}editor_3g
editorStatus="fault"
editorFileNum=1
editorRealFileNum=0
#最大重试次数,设为0则不重试
maxRetry=10
retry=0
#重试间隔(s)
interval=180
#editor
function getAndCheckEditor(){
   if [  -d $editorLocalDestDir ];then
        rm  $editorLocalDestDir/*
	else
	    mkdir -p $editorLocalDestDir
	fi
	for editorIp in $editorIps;do
		echo getEditor from $editorIp	
		editorLocalFile=$editorLocalDestDir/${yesterday}.${editorIp}.editor
		/usr/bin/rsync -avzP root@$editorIp::dutyeditor/DutyEditorStatistic${yesterday} $editorLocalFile
	done
  	hadoopDirPrepare $editorHadoopDestDir $yesterday
    ${HADOOP} fs -put $editorLocalDestDir/* $editorHadoopDestDir/$yesterday/

	editorRealFileNum=`${HADOOP} fs -du $editorHadoopDestDir/$yesterday/ | grep $yesterday -c`
	if [ $editorRealFileNum -ge $editorFileNum ];then
	    editorStatus="success"
	fi
}


function prepareEditorlog(){
	while [ "$editorStatus" == "fault" -a $retry -le $maxRetry ];do
        getAndCheckEditor
		if [ "$editorStatus" == "fault" ];then
			retry=$((retry+1))
			echo getEditorLog sleep, retry=$retry/$maxRetry fileCount=$editorRealFileNum/$editorFileNum
			sleep $interval"s"
		fi
	done
}

function main(){  
    prepareEditorlog	
    if [ "$editorStatus" == "fault" ];then
    	errorAlarm getEditorLog:articleFileCount=$editorRealFileNum/$editorFileNum
		exit 1
	else
		exit 0
	fi
}

main