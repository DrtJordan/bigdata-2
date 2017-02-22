#!/bin/bash

baseDir=$(cd "$(dirname "$0")"; pwd)
source $baseDir/../weblogHeader.sh

export JAVA_HOME=/usr/lib/jvm/java-6-sun/
export HADOOP_HOME=/home/weblog/hadoop
export HADOOP_CONF_DIR=/home/weblog/hadoop/etc/hadoop/

echo $yesterday 

/home/weblog/hadoop/bin/hadoop jar /home/weblog/rui/webAnalysis/weblogOffline-0.1.0-jar-with-dependencies.jar com.netease.jurassic.hadoopjob.RunMRJob -force -c com.netease.weblogOffline.statistics.sessionpath.FilterResultMR  -d $yesterday




