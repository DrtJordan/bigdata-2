#! /bin/sh
#sh yuegao.sh /home/weblog/gaojinnan/yuegao/201504/ 10

JAR=/home/weblog/webAnalysis/webAnalysis/weblogOffline-0.1.0-jar-with-dependencies.jar

/home/weblog/hadoop/bin/hadoop jar $JAR com.netease.weblogOffline.tools.GetChargeUrlData $@