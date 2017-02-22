#!/bin/bash

if [ $# = 0 ]; then
   premonth=`date +%Y%m -d "-1month"`
else
   premonth=$1
fi
   suffixes=_fee_article.data
   localDestDir=${WEBLOG_RESULT_LOCAL_DIR}weblogRsync/yuegao
   hadoopDestDir=/ntes_weblog/weblog/temp/YueGaoAnalysis/article
   /usr/bin/rsync -au 61.135.251.68::articlefee/$premonth${suffixes} $localDestDir/
 function putToHadoop(){
	hadoopDateDirExist=`${HADOOP} fs -ls $hadoopDestDir | grep -c $premonth`
	if [ $hadoopDateDirExist -eq 0 ];then
		${HADOOP} fs -mkdir -p $hadoopDestDir/$premonth
	fi
	${HADOOP} fs -put  $localDestDir/$premonth${suffixes} $hadoopDestDir/$premonth/
}

putToHadoop

export JAVA_HOME=${JAVA_HOME}
export HADOOP_HOME=${HADOOP_HOME}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}
baseDir=$(cd "$(dirname "$0")"; pwd)

source $baseDir/weblogHeader.sh

echo $yesterday

jar=${JAR}
sh $baseDir/runMRJob.sh -c com.netease.weblogOffline.statistics.yuegaoshuju.YueGaoAnalysisMR  -d $premonth --days 10

