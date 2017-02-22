
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
 
#gentieVoteIps="220.181.29.120 220.181.98.74 220.181.98.165 220.181.98.166 123.126.62.84 123.126.62.135"
gentieVoteIps="10.164.149.186 10.164.149.187 10.164.149.188 10.164.149.189 10.164.149.190"
gentieVoteLocalDestDir=${WEBLOG_RESULT_LOCAL_DIR}weblogRsync/genTieVote/$yesterday
gentieVoteHadoopDestDir=${WEBLOG_COMMMON_HDFS_DIR}genTieVoteLog
genTieVoteStatus="fault"
gentieVoteFileNum=5
gentieVoteRealFileNum=0

#最大重试次数,设为0则不重试
maxRetry=10
retry=0
#重试间隔(s)
interval=180

function getAndCheckGenTieVote(){

	if [  -d $gentieVoteLocalDestDir ];then
	    rm  $gentieVoteLocalDestDir/*
	else 
		mkdir -p $gentieVoteLocalDestDir
	fi
	
	for gentieVoteIp in $gentieVoteIps;do
		echo getGenTieVote from $gentieVoteIp	
		gentielocalVoteFile=$gentieVoteLocalDestDir/genTieVote_${yesterday}_$gentieVoteIp
		#/usr/bin/rsync -au $gentieVoteIp::resinlog/replyVote.log.$yest.log $gentielocalVoteFile
		rsync -av --password-file=/home/weblog/webAnalysis/data/weblogRsync/secrets/rsyncd.secrets_genTieNew gentie@$gentieVoteIp::pub2/add_vote.log_${yesterday} ${gentielocalVoteFile}"_pub2"
	done
	
    hadoopDirPrepare $gentieVoteHadoopDestDir $yesterday
	${HADOOP} fs -put $gentieVoteLocalDestDir/*$yesterday* $gentieVoteHadoopDestDir/$yesterday/
	
	gentieVoteRealFileNum=`${HADOOP} fs -du $gentieVoteHadoopDestDir/$yesterday/ | grep $yesterday -c`
	if [ $gentieVoteRealFileNum -ge $gentieVoteFileNum ];then
	   genTieVoteStatus="success"
	fi
}

function prepareGentieVotelog(){
	while [ "$genTieVoteStatus" == "fault" -a $retry -le $maxRetry ];do
        getAndCheckGenTieVote
		if [ "$genTieVoteStatus" == "fault" ];then
			retry=$((retry+1))
			echo getGenTieVotelog sleep, retry=$retry/$maxRetry fileCount=$gentieVoteRealFileNum/$gentieVoteFileNum
			sleep $interval"s"
		fi
	done
}


# gentieVote
function main(){
prepareGentieVotelog
    if [ "$genTieVoteStatus" == "fault" ];then
        errorAlarm getGenTieVotelog:genTieVoteFileCount=$gentieVoteRealFileNum/$gentieVoteFileNum
		exit 1
	else
		exit 0
	fi
}

main
