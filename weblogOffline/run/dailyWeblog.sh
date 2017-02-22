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
source $baseDir/weblogHeader.sh
echo $yesterday
listDataDest=${WEBLOG_RESULT_LOCAL_DIR}list

if [ ! -d $listDataDest ];then
    mkdir -p $listDataDest
fi

beforeRunMRMoudle dailyWeblog

sh $baseDir/runMRJob.sh -c com.netease.weblogOffline.statistics.dailyWeblog.WeblogPvUvMR -d $yesterday

sh $baseDir/runMRJob.sh -c com.netease.weblogOffline.statistics.dailyWeblog.Weblog163HomeIOMR -d $yesterday
${HADOOP} fs -text ${WEBLOG_OTHER_RESULT_HDFS_DIR}163HomeIO/$yesterday/p* | grep "refDomain_" | sort -k 2 -nr | awk '{print $1"\t"$2}' > $listDataDest/urlIsMainPageRefDomain$yesterday
${HADOOP} fs -text ${WEBLOG_OTHER_RESULT_HDFS_DIR}163HomeIO/$yesterday/p* | grep "urlDomain_" | sort -k 2 -nr | awk '{print $1"\t"$2}' > $listDataDest/urlIsMainPageUrlDomain$yesterday

sh $baseDir/runMRJob.sh -c com.netease.weblogOffline.statistics.dailyWeblog.BounceRateMR -d $yesterday 
${HADOOP} fs -text ${WEBLOG_OTHER_RESULT_HDFS_DIR}bounceRate/$yesterday/p* | sort -k 2 -nr | awk '{print $1"\t"$2}' > $listDataDest/bounceRate$yesterday

sh $baseDir/runMRJob.sh -c com.netease.weblogOffline.statistics.dailyWeblog.ShareBackStatisticsMR -d $yesterday
${HADOOP} fs -text ${WEBLOG_OTHER_RESULT_HDFS_DIR}shareBackRes/$yesterday/p* | awk -F'_' '{print $1"\t"$2"\t"$3"\t"$4}' |awk '{if($3=="s") print $1"\t"$2"\t"$4"\t"$5"\t"$6}' | sort > $listDataDest/weblogShare$yesterday
${HADOOP} fs -text ${WEBLOG_OTHER_RESULT_HDFS_DIR}shareBackRes/$yesterday/p* | awk -F'_' '{print $1"\t"$2"\t"$3"\t"$4}' |awk '{if($3=="b") print $1"\t"$2"\t"$4"\t"$5"\t"$6}' | sort > $listDataDest/weblogBack$yesterday

sh $baseDir/runMRJob.sh -c com.netease.weblogOffline.statistics.dailyWeblog.ShareBackTopNMR -d $yesterday
${HADOOP} fs -text ${WEBLOG_OTHER_RESULT_HDFS_DIR}shareBackTopN/$yesterday/p* | awk -F'_' '{print $1"\t"$2"\t"$3"\t"$4}' |awk '{if($3=="s") print $1"\t"$2"\t"$4"\t"$5"\t"$6}' > $listDataDest/shareTopN$yesterday
${HADOOP} fs -text ${WEBLOG_OTHER_RESULT_HDFS_DIR}shareBackTopN/$yesterday/p* | awk -F'_' '{print $1"\t"$2"\t"$3"\t"$4}' |awk '{if($3=="b") print $1"\t"$2"\t"$4"\t"$5"\t"$6}' > $listDataDest/backTopN$yesterday

sh $baseDir/runMRJob.sh -c com.netease.weblogOffline.statistics.dailyWeblog.FirstOpTimeMR -d $yesterday -input /ntes_weblog/commonData/weblog/$yesterday -output ${WEBLOG_TEMP_HDFS_DIR}firstOpTime/$yesterday,$listDataDest/firstOpTime


#导出计算大数据日志频道首页，频道整体专题的pvuv等指标
${HADOOP} fs -text ${WEBLOG_OTHER_RESULT_HDFS_DIR}ChannelHomeFromWeblog/$yesterday/p*  | sort -k 1 -nr > $listDataDest/ChannelHomeFromWeblog$yesterday

#原创同步到列表数据
cat ${WEBLOG_RESULT_LOCAL_DIR}weblogRsync/ycInfo/${yesterday}* > $listDataDest/ycInfo$yesterday


afterRunMRMoudle dailyWeblog 3 600 

if [ "$errorList" != "" ];then
	errorAlarm dailyWeblog:$errorList
fi
 







