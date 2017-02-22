#!/bin/bash

baseDir=$(cd "$(dirname "$0")"; pwd)
source $baseDir/../weblogHeader.sh

export JAVA_HOME=${JAVA_HOME}
export HADOOP_HOME=${HADOOP_HOME}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}

echo $yesterday

beforeRunMRMoudle mediaDaily
#sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.media.UrlMediaMergeMR -d $yesterday

#计算媒体数据,依赖子媒文章（发布系统数据）
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.media.MediaMergeMR -d $yesterday

##媒体数据存储本地
sh $baseDir/expmedia.sh $yesterday 
afterRunMRMoudle mediaDaily 3 600 

if [ "$errorList" != "" ];then
	errorAlarm mediaDaily:$errorList
fi





