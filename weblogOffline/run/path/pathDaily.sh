#!/bin/bash

baseDir=$(cd "$(dirname "$0")"; pwd)
source $baseDir/../weblogHeader.sh

export JAVA_HOME=/usr/lib/jvm/java-6-sun/
export HADOOP_HOME=/home/weblog/hadoop
export HADOOP_CONF_DIR=/home/weblog/hadoop/etc/hadoop/

echo $yesterday

##获得入口数据及非入口数据
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.path.WeblogEnterMR  -d $yesterday
/home/weblog/hadoop/bin/hadoop fs -rmr /ntes_weblog/path/midLayer/pathentry/$yesterday/
/home/weblog/hadoop/bin/hadoop fs -rmr /ntes_weblog/path/midLayer/pathnotentry/$yesterday/
/home/weblog/hadoop/bin/hadoop fs -mkdir -p /ntes_weblog/path/midLayer/pathentry/$yesterday/
/home/weblog/hadoop/bin/hadoop fs -mkdir -p /ntes_weblog/path/midLayer/pathnotentry/$yesterday/

/home/weblog/hadoop/bin/hadoop fs -cp /ntes_weblog/path/midLayer/enterornot/$yesterday/entry* /ntes_weblog/path/midLayer/pathentry/$yesterday/
/home/weblog/hadoop/bin/hadoop fs -cp /ntes_weblog/path/midLayer/enterornot/$yesterday/notentry* /ntes_weblog/path/midLayer/pathnotentry/$yesterday/


#计算访问路径
##统计站外到入口页的数据
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.path.SourceEnterVisitPathMR  -d $yesterday
##统计入口页到一级页面的数据
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.path.EnterFirstVisitPathMR  -d $yesterday
##统计入口页的流失数量（从站外来的pgr在后续没有成为其他页面的prev_pgr）
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.path.MergeFirstLossMR  -d $yesterday



