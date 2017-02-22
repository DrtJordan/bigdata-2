
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

gentieBaseIps="220.181.29.96 61.135.251.244"
gentieBaseLocalDestDir=${WEBLOG_RESULT_LOCAL_DIR}weblogRsync/genTieBase/$yesterday
gentieBaseHadoopDestDir=${WEBLOG_COMMMON_HDFS_DIR}genTieBaseIncr
genTieBaseStatus="fault"
gentieBaseFileNum=2
gentieBaseRealFileNum=0

#最大重试次数,设为0则不重试
maxRetry=10
retry=0
#重试间隔(s)
interval=360

function getAndCheckGenTieBase(){

	if [  -d $gentieBaseLocalDestDir ];then
	    rm  $gentieBaseLocalDestDir/*
	else 
		mkdir -p $gentieBaseLocalDestDir
	fi
	
	for gentieBaseIp in $gentieBaseIps;do
		echo getGentieBase from $gentieBaseIp	
		gentieBaseLocalFile=$gentieBaseLocalDestDir/genTieBase_${yesterday}_$gentieBaseIp
		/usr/bin/rsync -au --port=874 $gentieBaseIp::data/thread_${yesterday}.log $gentieBaseLocalFile
	done
	
    hadoopDirPrepare $gentieBaseHadoopDestDir $yesterday
	${HADOOP} fs -put $gentieBaseLocalDestDir/*$yesterday* $gentieBaseHadoopDestDir/$yesterday/
	
	gentieBaseRealFileNum=`${HADOOP} fs -du $gentieBaseHadoopDestDir/$yesterday/ | grep $yesterday -c`
	if [ $gentieBaseRealFileNum -ge $gentieBaseFileNum ];then
	   genTieBaseStatus="success"
	fi
}

function prepareGentieBase(){
	while [ "$genTieBaseStatus" == "fault" -a $retry -le $maxRetry ];do
        getAndCheckGenTieBase
		if [ "$genTieBaseStatus" == "fault" ];then
			retry=$((retry+1))
			echo getGenTieBaselog sleep, retry=$retry/$maxRetry fileCount=$gentieBaseRealFileNum/$gentieBaseFileNum
			sleep $interval"s"
		fi
	done
}


# gentie
function main(){
prepareGentieBase
    if [ "$genTieBaseStatus" == "fault" ];then
        errorAlarm getGenTieBase:genTieBaseFileCount=$gentieBaseRealFileNum/$gentieBaseFileNum
		exit 1
	else
		exit 0
	fi
}

main
