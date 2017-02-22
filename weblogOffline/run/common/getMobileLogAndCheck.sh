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

mobileLocalDestDir=${WEBLOG_RESULT_LOCAL_DIR}weblogRsync/mobile/$yesterday
MobileStatus="fault"
MobilefileNum=1


#最大重试次数,设为0则不重试
maxRetry=10
retry=0
#重试间隔(s)
interval=180

#yc
function getAndCheckMobile(){
 if [  -d $mobileLocalDestDir ];then
        rm  $mobileLocalDestDir/*
	else 
		mkdir -p $mobileLocalDestDir
	fi
	/usr/bin/rsync -au 123.58.179.93::checkdata/mobi_${yesterday} $mobileLocalDestDir/	
  
    
    MobileRealFileNum=`ls -l $mobileLocalDestDir/ | grep mobi_${yesterday} -c`
	if [ $MobileRealFileNum -ge $MobilefileNum ];then
	    MobileStatus="success"
	fi

}


function prepareMobilelog(){
	while [ "$MobileStatus" == "fault" -a $retry -le $maxRetry ];do
        getAndCheckMobile
		if [ "$MobileStatus" == "fault" ];then
			retry=$((retry+1))
			echo getMobilelog sleep, retry=$retry/$maxRetry fileCount=$MobileRealFileNum/$MobilefileNum
			sleep $interval"s"
		fi
	done
	
}

function main(){  
prepareMobilelog	
   if [ "$MobileStatus" == "fault" ];then
   		errorAlarm getMobilelog:MobileFileCount=$MobileRealFileNum/$MobilefileNum
		exit 1
	else
		exit 0
	fi
}

main