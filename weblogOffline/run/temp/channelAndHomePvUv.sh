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

jar=/home/weblog/liyonghong/webAnalysis/weblogOffline-0.1.0-jar-with-dependencies.jar
/home/weblog/hadoop/bin/hadoop jar $jar com.netease.jurassic.hadoopjob.RunMRJob -force -c com.netease.weblogOffline.temp.ChannelAndHomePvUvFromZyMR  -d $yesterday

/home/weblog/hadoop/bin/hadoop jar $jar com.netease.jurassic.hadoopjob.RunMRJob -force -c com.netease.weblogOffline.temp.ChannelContentScoreMR  -d $yesterday






