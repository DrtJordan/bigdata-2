#!/bin/bash

if [ $# = 0 ]; then
    yesterday=`date +%Y%m%d -d "-1days"`
else
    yesterday=$1
fi

export JAVA_HOME=/usr/lib/jvm/java-6-sun/
export HADOOP_HOME=/home/weblog/hadoop
export HADOOP_CONF_DIR=/home/weblog/hadoop/etc/hadoop/
baseDir=$(cd "$(dirname "$0")"; pwd)

echo $yesterday

jiguangDataDest=/home/weblog/webAnalysis/data/toJiGuang/$yesterday
if [ ! -d $jiguangDataDest ];then
    mkdir -p $jiguangDataDest
fi

sh $baseDir/runMRJob.sh -c com.netease.weblogOffline.midlayer.contentscore.ShareBackCountMR -d $yesterday
sh $baseDir/runMRJob.sh -c com.netease.weblogOffline.midlayer.contentscore.ContentPvUvMR -d $yesterday
sh $baseDir/runMRJob.sh -c com.netease.weblogOffline.midlayer.contentscore.DailyGenTieCountMR -d $yesterday
sh $baseDir/runMRJob.sh -c com.netease.weblogOffline.midlayer.contentscore.DailyYcInfoMergeMR -d $yesterday
sh $baseDir/runMRJob.sh -c com.netease.weblogOffline.midlayer.contentscore.ContentScoreVectorMergeMR -d $yesterday

sh $baseDir/runMRJob.sh -c com.netease.weblogOffline.midlayer.contentscore.DailyGenTieGroupUvMR -d $yesterday
sh $baseDir/runMRJob.sh -c com.netease.weblogOffline.statistics.ChannelHomeFromWeblogMR -d $yesterday
sh $baseDir/runMRJob.sh -c com.netease.weblogOffline.statistics.ChannelContentScoreMR -d $yesterday

/home/weblog/hadoop/bin/hadoop fs -text /ntes_weblog/weblog/statistics/result_other/channelContentScore/$yesterday/p* > $jiguangDataDest/${yesterday}_influence.txt

sh $baseDir/runMRJob.sh -c com.netease.weblogOffline.midlayer.contentscore.ContentScoreVectorExportMR -d $yesterday --days 30
/home/weblog/hadoop/bin/hadoop fs -text /ntes_weblog/weblog/statistics/result_other/ycDetail/$yesterday/p* > $jiguangDataDest/${yesterday}_ycDetail.txt

#toJiGuang
/usr/bin/rsync -au $jiguangDataDest/$yesterday* 223.252.196.104::kpi
 








