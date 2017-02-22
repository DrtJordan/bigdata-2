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

beforeRunMRMoudle Kaola

#网站导入考拉流量各个指标监控
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.tokaola.ToKaolaMR  -d $yesterday

#网站导入考拉流量各个URL数量统计
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.tokaola.InfoOfUrlMR  -d $yesterday


#zy own URL数量统计
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.tokaola.ZyUrlPVUVMR  -d $yesterday

#zy own URL  group by html own
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.tokaola.OwnUrlPVUVMR  -d $yesterday


#网站导入考拉流量各个URL pvuv
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.tokaola.KaolaUrlPVUVMR  -d $yesterday

#网站导入考拉Email统计
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.tokaola.DistinctEmailMR  -d $yesterday

${HADOOP} fs -libjars ${JAR} -text ${WEBLOG_OTHER_RESULT_HDFS_DIR}kaola/$yesterday/p* |awk -F'\t' '{print $1}'| awk -F',' '{print $1"\t"$2"\t"$3"\t"$4"\t"$5"\t"$6"\t"$7"\t"$8}' | sort -k 8 -nr > $listDataDest/kaolaurlinfo$yesterday

${HADOOP} fs -text ${WEBLOG_OTHER_RESULT_HDFS_DIR}kaolapvuv/$yesterday/p* |awk -F'\t' '{print $1}'| awk -F',' '{print $1"\t"$2"\t"$3}' | sort -k 2 -nr > $listDataDest/kaolapvuv$yesterday
${HADOOP} fs -text ${WEBLOG_OTHER_RESULT_HDFS_DIR}groupbyownpvuv/$yesterday/p* |awk -F'\t' '{print $1}'| awk -F',' '{print $1"\t"$2"\t"$3}' | sort -k 2 -nr > $listDataDest/groupbyownpvuv$yesterday


 afterRunMRMoudle Kaola 3 600 
 
if [ "$errorList" != "" ];then
	errorAlarm Kaola:$errorList
fi



