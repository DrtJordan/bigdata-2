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
urlMediachannels="0001 0003 0005 0006 0008 0009 0011 0016 0025 0026 0036 0029 0086 0087 0092"
urlMediaLocalDir=${WEBLOG_RESULT_LOCAL_DIR}media/urlMedia/$yesterday
urlMediaHadoopDestDir=${WEBLOG_COMMMON_HDFS_DIR}articleIncr_web
urlMediaStatus="fault"

urlMediafileNum=15
MobileRealFileNum=0

#最大重试次数,设为0则不重试
maxRetry=10
retry=0
#重试间隔(s)
interval=360

#yc
function getAndCheckUrlMedia(){
   if [  -d $urlMediaLocalDir ];then
    rm  $urlMediaLocalDir/*
	else 
	mkdir -p $urlMediaLocalDir
    fi

   cd $urlMediaLocalDir
  for channel in $urlMediachannels;do
    echo $channel
    temp_1=$channel"_count.temp1"
    #temp_2=$channel"_count.temp2"
    dest=$channel"_count.txt"
    wget http://220.181.98.199/devilfish/$channel/$yest/count.txt -O $temp_1
    iconv -f "gbk" -t "utf-8" < $temp_1 > $dest
    
    rm $temp_1
  done
  
  	hadoopDirPrepare $urlMediaHadoopDestDir $yesterday
    ${HADOOP} fs -put $urlMediaLocalDir/* $urlMediaHadoopDestDir/$yesterday/
    
}


function prepareUrlMedialog(){
        getAndCheckUrlMedia
}

function main(){  
        prepareUrlMedialog	
		exit 0
}

main