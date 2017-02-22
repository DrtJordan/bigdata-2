
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
gentieIps="220.181.29.120 220.181.29.52 220.181.98.74 61.135.251.155 123.126.62.84 123.126.62.135 123.126.62.138"
gentieLocalDestDir=${WEBLOG_RESULT_LOCAL_DIR}weblogRsync/genTie/$yesterday
gentieHadoopDestDir=${WEBLOG_COMMMON_HDFS_DIR}genTieLog
genTieStatus="fault"
gentiefileNum=7
gentieRealFileNum=0

#最大重试次数,设为0则不重试
maxRetry=10
retry=0
#重试间隔(s)
interval=180

function getAndCheckGenTie(){

	if [  -d $gentieLocalDestDir ];then
	    rm  $gentieLocalDestDir/*
	else 
		mkdir -p $gentieLocalDestDir
	fi
	
	for gentieIp in $gentieIps;do
		echo getGenTie from $gentieIp	
		gentielocalFile=$gentieLocalDestDir/genTie_${yesterday}_$gentieIp
		/usr/bin/rsync -au $gentieIp::resinlog/replyPost.log.$yest.log $gentielocalFile
	done
	
    hadoopDirPrepare $gentieHadoopDestDir $yesterday
	${HADOOP} fs -put $gentieLocalDestDir/*$yesterday* $gentieHadoopDestDir/$yesterday/
	
	gentieRealFileNum=`${HADOOP} fs -du $gentieHadoopDestDir/$yesterday/ | grep $yesterday -c`
	if [ $gentieRealFileNum -ge $gentiefileNum ];then
	   genTieStatus="success"
	fi
}

function prepareGentielog(){
	while [ "$genTieStatus" == "fault" -a $retry -le $maxRetry ];do
        getAndCheckGenTie
		if [ "$genTieStatus" == "fault" ];then
			retry=$((retry+1))
			echo getGenTielog sleep, retry=$retry/$maxRetry fileCount=$gentieRealFileNum/$gentiefileNum
			sleep $interval"s"
		fi
	done
}


# gentie
function main(){
prepareGentielog
    if [ "$genTieStatus" == "fault" ];then
        errorAlarm getGenTielog:genTieFileCount=$gentieRealFileNum/$gentiefileNum
		exit 1
	else
		exit 0
	fi
}

main
