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
listDataDest=${WEBLOG_RESULT_LOCAL_DIR}list

if [ ! -d $listDataDest ];then
    mkdir -p $listDataDest
fi
beforeRunMRMoudle gentieinfo

#跟帖分端各个指标监控
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.gentieInfo.GenTieChannelSourceCountUvMR  -d $yesterday
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.gentieInfo.GenTieSourceUvMR  -d $yesterday
#跟帖URL数量统计
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.gentieInfo.GenTieChannelTopurlMR -d $yesterday

${HADOOP} fs -text ${WEBLOG_OTHER_RESULT_HDFS_DIR}GenTieChannelSourceCountUv/$yesterday/p* |awk -F'\t' '{print $1"\t"$2"\t"$3"\t"$4"\t"$5"\t"$6"\t"$7}' | sort -k 1 -nr > $listDataDest/GenTieChannelSourceCount$yesterday
${HADOOP} fs -text ${WEBLOG_OTHER_RESULT_HDFS_DIR}GenTieChannelSourceCountUv/$yesterday/p* |awk -F'\t' '{print $1"\t"$8"\t"$9"\t"$10"\t"$11"\t"$12"\t"$13}' | sort -k 1 -nr > $listDataDest/GenTieChannelSourceUv$yesterday
${HADOOP} fs -text ${WEBLOG_OTHER_RESULT_HDFS_DIR}GenTieChannelTopurlTopN/$yesterday/p* |awk -F'\t' '{print $1"\t"$2"\t"$3}'> $listDataDest/GenTieChannelTopurlTopN$yesterday

${HADOOP} fs -text ${WEBLOG_OTHER_RESULT_HDFS_DIR}GenTieSourceUv/$yesterday/p* |awk -F'\t' '{print $2"\t"$3"\t"$4"\t"$5"\t"$6}' > $listDataDest/GenTieSourceUv$yesterday


afterRunMRMoudle gentieinfo 3 600 
if [ "$errorList" != "" ];then
	errorAlarm gentieinfo:$errorList
fi








