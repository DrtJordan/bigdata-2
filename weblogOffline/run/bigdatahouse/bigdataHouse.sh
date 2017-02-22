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

beforeRunMRMoudle BigdataHouse
# zy过滤日志转成统一的map
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.bigdatahouse.ZyToMapMR  -d $yesterday

# 原创数据汇总
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.bigdatahouse.DailyYcInfoMergeMR  -d $yesterday


# 文章数据汇总
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.bigdatahouse.UrlMediaMergeMR  -d $yesterday

# url 各项指标输出
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.bigdatahouse.PvUvShareBackMR  -d $yesterday
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.bigdatahouse.GentieCountAndUvMR  -d $yesterday 
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.bigdatahouse.IsOrNotYcMR  -d $yesterday 
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.bigdatahouse.MediaSourceMR  -d $yesterday 
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.bigdatahouse.DailyUrlInfoVector3MR  -d $yesterday 

afterRunMRMoudle BigdataHouse 3 600 

if [ "$errorList" != "" ];then
	errorAlarm BigdataHouse:$errorList
fi	




