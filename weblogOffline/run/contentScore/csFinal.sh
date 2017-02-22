#!/bin/bash

baseDir=$(cd "$(dirname "$0")"; pwd)

source $baseDir/../weblogHeader.sh

jiguangDataDest=${WEBLOG_RESULT_LOCAL_DIR}toJiGuang/$yesterday

if [ ! -d $jiguangDataDest ];then
    mkdir -p $jiguangDataDest
fi

beforeRunMRMoudle csFinal

sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.contentscore.GenTieYcUvMR -d $yesterday
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.contentscore.ChannelHomeFromWeblogMR -d $yesterday
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.contentscore.ContentScoreVectorMergeMR -d $yesterday
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.contentscore.ChannelContentScoreMR -d $yesterday
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.contentscore.ContentScoreVectorExportMR -d $yesterday --days 30

afterRunMRMoudle csFinal 3 600 

${HADOOP} fs -text ${WEBLOG_OTHER_RESULT_HDFS_DIR}channelContentScore/$yesterday/p* > $jiguangDataDest/${yesterday}_influence.txt
${HADOOP} fs -text ${WEBLOG_OTHER_RESULT_HDFS_DIR}ycDetail/$yesterday/p* > $jiguangDataDest/${yesterday}_ycDetail.txt

if [ "$errorList" != "" ];then
	errorAlarm csFinal:$errorList
else
	#toJiGuang
	/usr/bin/rsync -au $jiguangDataDest/$yesterday* 223.252.196.104::kpi
fi





 








