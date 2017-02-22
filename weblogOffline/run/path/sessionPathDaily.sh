#!/bin/bash

baseDir=$(cd "$(dirname "$0")"; pwd)
source $baseDir/../weblogHeader.sh

export JAVA_HOME=/usr/lib/jvm/java-6-sun/
export HADOOP_HOME=/home/weblog/hadoop
export HADOOP_CONF_DIR=/home/weblog/hadoop/etc/hadoop/

echo $yesterday 

##获得入口数据及非入口数据
/home/weblog/hadoop/bin/hadoop jar /home/weblog/rui/webAnalysis/weblogOffline-0.1.0-jar-with-dependencies.jar com.netease.jurassic.hadoopjob.RunMRJob -force -c com.netease.weblogOffline.statistics.sessionpath.WeblogEnterMR  -d $yesterday
/home/weblog/hadoop/bin/hadoop fs -rmr /ntes_weblog/path/midLayer/sessionpathentry/$yesterday/
/home/weblog/hadoop/bin/hadoop fs -rmr /ntes_weblog/path/midLayer/sessionpathnotentry/$yesterday/
/home/weblog/hadoop/bin/hadoop fs -mkdir -p /ntes_weblog/path/midLayer/sessionpathentry/$yesterday/
/home/weblog/hadoop/bin/hadoop fs -mkdir -p /ntes_weblog/path/midLayer/sessionpathnotentry/$yesterday/

/home/weblog/hadoop/bin/hadoop fs -cp /ntes_weblog/path/midLayer/sessionenterornot/$yesterday/entry* /ntes_weblog/path/midLayer/sessionpathentry/$yesterday/
/home/weblog/hadoop/bin/hadoop fs -cp /ntes_weblog/path/midLayer/sessionenterornot/$yesterday/notentry* /ntes_weblog/path/midLayer/sessionpathnotentry/$yesterday/


#计算访问路径
##统计站外到入口页到后续级别页面的路径数据
#按照时间排序的方式统计用户路径
/home/weblog/hadoop/bin/hadoop jar /home/weblog/rui/webAnalysis/weblogOffline-0.1.0-jar-with-dependencies.jar com.netease.jurassic.hadoopjob.RunMRJob -force -c com.netease.weblogOffline.statistics.sessionpath.SessionPathWithTimeMR  -d $yesterday
#按照prev_pgr与pgr匹配的方式统计用户路径
/home/weblog/hadoop/bin/hadoop jar /home/weblog/rui/webAnalysis/weblogOffline-0.1.0-jar-with-dependencies.jar com.netease.jurassic.hadoopjob.RunMRJob -force -c com.netease.weblogOffline.statistics.sessionpath.SessionPathWithPgrMR  -d $yesterday
#统计两种不同方式下用户路径的一致性
/home/weblog/hadoop/bin/hadoop jar /home/weblog/rui/webAnalysis/weblogOffline-0.1.0-jar-with-dependencies.jar com.netease.jurassic.hadoopjob.RunMRJob -force -c com.netease.weblogOffline.statistics.sessionpath.ComparePathWith2MethodsMR  -d $yesterday
#设置threshold,过滤按时间排序方式所得用户路径，过滤结果为三种：①。ref->first order;②：ref->first order->second order;③：ref->first order->second order->third order
/home/weblog/hadoop/bin/hadoop jar /home/weblog/rui/webAnalysis/weblogOffline-0.1.0-jar-with-dependencies.jar com.netease.jurassic.hadoopjob.RunMRJob -force -c com.netease.weblogOffline.statistics.sessionpath.FilterResultMR  -d $yesterday
#统计通过不同外站进入网易的优质用户的数量，优质用户为进入网易并在网易站内访问页面超过5页的用户，同时也记录了通过不同外站进入网易，在网易站内访问的页面数及相应的优质用户数
/home/weblog/hadoop/bin/hadoop jar /home/weblog/rui/webAnalysis/weblogOffline-0.1.0-jar-with-dependencies.jar com.netease.jurassic.hadoopjob.RunMRJob -force -c com.netease.weblogOffline.statistics.sessionpath.HighQualityUserRecordMR  -d $yesterday

