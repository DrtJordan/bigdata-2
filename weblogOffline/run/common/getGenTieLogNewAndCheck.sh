
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

#gentieIps="10.164.152.175 10.164.152.179"
gentieIps="10.164.149.99 10.164.149.100 10.164.149.143 10.164.149.144 10.164.149.146"
gentieLocalDestDir=${WEBLOG_RESULT_LOCAL_DIR}weblogRsync/genTieNew/$yesterday
gentieHadoopDestDir=${WEBLOG_COMMMON_HDFS_DIR}genTieLogNew
genTieStatus="fault"
gentiefileNum=5
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
		gentielocalFile=$gentieLocalDestDir/add_tie.log_${yesterday}_$gentieIp
        rsync -av --password-file=/home/weblog/webAnalysis/data/weblogRsync/secrets/rsyncd.secrets_genTieNew gentie@$gentieIp::pub1/add_tie.log_${yesterday} ${gentielocalFile}"_pub1"
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
        errorAlarm getGenTielogNew:genTieFileCount=$gentieRealFileNum/$gentiefileNum
		exit 1
	else
		exit 0
	fi
}

main
