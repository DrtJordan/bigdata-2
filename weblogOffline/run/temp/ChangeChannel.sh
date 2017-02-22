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

#计算各个频道 在各个版本下文章页的pvuv
/home/weblog/hadoop/bin/hadoop jar $jar com.netease.jurassic.hadoopjob.RunMRJob -force -c com.netease.weblogOffline.statistics.ChangeChannel.ChannelofBroadPvUvFromWeblogMR  -d $yesterday

#计算各个频道在各个版本下文章页 有Email的用户数 和没Email的用户数
/home/weblog/hadoop/bin/hadoop jar $jar com.netease.jurassic.hadoopjob.RunMRJob -force -c com.netease.weblogOffline.statistics.ChangeChannel.EmailOrNoEmailUvFromWeblogMR  -d $yesterday


# 计算各个频道在各个版本下文章页对应url的pv
/home/weblog/hadoop/bin/hadoop jar $jar com.netease.jurassic.hadoopjob.RunMRJob -force -c com.netease.weblogOffline.statistics.ChangeChannel.ChannelBroadurlPvMR  -d $yesterday


# 计算各个频道在各个版本下文章页对应url的sidcount
/home/weblog/hadoop/bin/hadoop jar $jar com.netease.jurassic.hadoopjob.RunMRJob -force -c com.netease.weblogOffline.statistics.ChangeChannel.ChannelBroadurlSIDCountMR  -d $yesterday

# 计算各个频道在各个版本下click事件文章页对应的url
/home/weblog/hadoop/bin/hadoop jar $jar com.netease.jurassic.hadoopjob.RunMRJob -force -c com.netease.weblogOffline.statistics.ChangeChannel.ChannelClickUrlMR  -d $yesterday
# 计算各个频道在各个版本下click事件总和 以及 涉及url pv
/home/weblog/hadoop/bin/hadoop jar $jar com.netease.jurassic.hadoopjob.RunMRJob -force -c com.netease.weblogOffline.statistics.ChangeChannel.ChannelClickUrlCountAndUrlPv  -d $yesterday
# 计算各个频道在各个版本下退出文章页对应的url
/home/weblog/hadoop/bin/hadoop jar $jar com.netease.jurassic.hadoopjob.RunMRJob -force -c com.netease.weblogOffline.statistics.ChangeChannel.ChannelBroadExitUrlMR  -d $yesterday


# 计算各个频道在各个版本下退出文章页总和 以及 涉及url pv
/home/weblog/hadoop/bin/hadoop jar $jar com.netease.jurassic.hadoopjob.RunMRJob -force -c com.netease.weblogOffline.statistics.ChangeChannel.ChannelExitUrlCountAndUrlPv  -d $yesterday

# 计算各个频道在各个版本下跳出文章页对应的url
/home/weblog/hadoop/bin/hadoop jar $jar com.netease.jurassic.hadoopjob.RunMRJob -force -c com.netease.weblogOffline.statistics.ChangeChannel.ChannelBounceUrlMR  -d $yesterday

# 计算各个频道在各个版本下跳出文章页总和 以及 涉及url pv
/home/weblog/hadoop/bin/hadoop jar $jar com.netease.jurassic.hadoopjob.RunMRJob -force -c com.netease.weblogOffline.statistics.ChangeChannel.ChannelBounceUrlCountAndSIDCount  -d $yesterday

# 计算各个频道在各个版本下文章页uuid ViewFocus事件对应的次数
/home/weblog/hadoop/bin/hadoop jar $jar com.netease.jurassic.hadoopjob.RunMRJob -force -c com.netease.weblogOffline.statistics.ChangeChannel.ChannelBroadViewFocusCountMR  -d $yesterday
# 计算各个频道在各个版本下文章页 channelName+"_"+broad,pgr,uuid  对应的最大pagescrolly
/home/weblog/hadoop/bin/hadoop jar $jar com.netease.jurassic.hadoopjob.RunMRJob -force -c com.netease.weblogOffline.statistics.ChangeChannel.ChannelBroadMaxPagescrollYCountMR  -d $yesterday

# 计算旅游频道下文章页 ip分布
/home/weblog/hadoop/bin/hadoop jar $jar com.netease.jurassic.hadoopjob.RunMRJob -force -c com.netease.weblogOffline.statistics.ChangeChannel.IPMR  -d $yesterday










