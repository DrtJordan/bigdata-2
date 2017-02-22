#! /bin/bash
if [ $# = 0 ]; then
    yesterday=`date +%Y%m%d -d "-1days"`
else
    yesterday=$1
fi

user="weblog"
export JAVA_HOME=${JAVA_HOME}
export HADOOP_HOME=${HADOOP_HOME}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}

echo $yesterday
kinit -k -t /home/weblog/weblog.keytab weblog/dev;kinit -R

#大数据日志
/home/weblog/hive-0.12.0/bin/hive -S -e "use weblog;add jar ${JAR};ALTER TABLE weblog ADD if not exists PARTITION (dt='$yesterday') location '${WEBLOG_DIR}/$yesterday';exit;"

#章鱼日志
/home/weblog/hive-0.12.0/bin/hive -S -e "use weblog;add jar ${JAR};ALTER TABLE zylog  ADD if not exists PARTITION (dt='$yesterday') location '${ZYLOG_DIR}/$yesterday';exit;"

#跟帖数据日志
#/home/weblog/hive-0.12.0/bin/hive -S -e "use weblog;add jar ${JAR};ALTER TABLE gentieFilter ADD if not exists PARTITION (dt='$yesterday') location '/ntes_weblog/weblog/statistics/temp/GenTieUCPDPUMR/$yesterday';exit;"

#大数据日志

/home/weblog/hive-0.12.0/bin/hive -S -e "use weblog;add jar ${JAR};ALTER TABLE weblogFilter ADD if not exists PARTITION (dt='$yesterday') location '${WEBLOG_COMMMON_HDFS_DIR}weblog/$yesterday';exit;"

/home/weblog/hive-0.12.0/bin/hive -S -e "use weblog;add jar ${JAR};ALTER TABLE wap_zylog ADD if not exists PARTITION (dt='$yesterday') location '${WEBLOG_COMMMON_HDFS_DIR}devilfishLogClassification/wap/$yesterday';exit;"
delateday=`date  +%Y%m%d -d "-31days"`

#bigdatahouse 大数据仓库

#原创数据
/home/weblog/hive-0.12.0/bin/hive -S -e "use bigdatahouse;add jar ${JAR};ALTER TABLE YcInfo ADD if not exists PARTITION (dt='$yesterday') location '${WEBLOG_COMMMON_MIDLAYER_HDFS_DIR}originalContentAll/$yesterday';exit;"
#文章数据
/home/weblog/hive-0.12.0/bin/hive -S -e "use bigdatahouse;add jar ${JAR};ALTER TABLE ArticleInfo ADD if not exists PARTITION (dt='$yesterday') location '${WEBLOG_COMMMON_MIDLAYER_HDFS_DIR}articleAll_web/$yesterday';exit;"

bigdataHouseDelateday=`date  +%Y%m%d -d "-2days"`
#原创删除前天数据
/home/weblog/hive-0.12.0/bin/hive -S -e "use bigdatahouse;add jar ${JAR};ALTER TABLE YcInfo DROP if  exists PARTITION (dt='$bigdataHouseDelateday');exit;"
#文章删除前天数据
/home/weblog/hive-0.12.0/bin/hive -S -e "use bigdatahouse;add jar ${JAR};ALTER TABLE ArticleInfo DROP if  exists PARTITION (dt='$bigdataHouseDelateday');exit;"


#跟帖数据
/home/weblog/hive-0.12.0/bin/hive -S -e "use bigdatahouse;add jar ${JAR};ALTER TABLE GenTieLog ADD if not exists PARTITION (dt='$yesterday') location '${WEBLOG_COMMMON_HDFS_DIR}genTieLog/$yesterday';exit;"

#StatisticsOfUrl
/home/weblog/hive-0.12.0/bin/hive -S -e "use bigdatahouse;add jar ${JAR};ALTER TABLE OriginUrlStatistics ADD if not exists PARTITION (dt='$yesterday') location '${WEBLOG_COMMMON_MIDLAYER_HDFS_DIR}dailyUrlInfoVector/$yesterday/url';exit;"

#StatisticsOfPureUrl
/home/weblog/hive-0.12.0/bin/hive -S -e "use bigdatahouse;add jar ${JAR};ALTER TABLE PureUrlStatistics ADD if not exists PARTITION (dt='$yesterday') location '${WEBLOG_COMMMON_MIDLAYER_HDFS_DIR}dailyUrlInfoVector/$yesterday/pureurl';exit;"
#zylogFilter数据
/home/weblog/hive-0.12.0/bin/hive -S -e "use bigdatahouse;add jar ${JAR};ALTER TABLE zylogfilter ADD if not exists PARTITION (dt='$yesterday') location '${WEBLOG_COMMMON_HDFS_DIR}devilfishLog/$yesterday';exit;"
#weblogFilter数据
/home/weblog/hive-0.12.0/bin/hive -S -e "use bigdatahouse;add jar ${JAR};ALTER TABLE weblogfilter ADD if not exists PARTITION (dt='$yesterday') location '${WEBLOG_COMMMON_HDFS_DIR}weblog/$yesterday';exit;"


/home/weblog/hive-0.12.0/bin/hive -S -e "use bigdatahouse;add jar ${JAR};ALTER TABLE GenTieLog DROP if  exists PARTITION (dt='$delateday');exit;"
/home/weblog/hive-0.12.0/bin/hive -S -e "use bigdatahouse;add jar ${JAR};ALTER TABLE OriginUrlStatistics DROP if  exists PARTITION (dt='$delateday');exit;"
/home/weblog/hive-0.12.0/bin/hive -S -e "use bigdatahouse;add jar ${JAR};ALTER TABLE PureUrlStatistics DROP if  exists PARTITION (dt='$delateday');exit;"
/home/weblog/hive-0.12.0/bin/hive -S -e "use bigdatahouse;add jar ${JAR};ALTER TABLE zylogfilter DROP if  exists PARTITION (dt='$delateday');exit;"
/home/weblog/hive-0.12.0/bin/hive -S -e "use bigdatahouse;add jar ${JAR};ALTER TABLE zylogfilter DROP if exists PARTITION (dt='$delateday');exit;"
/home/weblog/hive-0.12.0/bin/hive -S -e "use bigdatahouse;add jar ${JAR};ALTER TABLE weblogfilter DROP if exists PARTITION (dt='$delateday');exit;"


${HADOOP} fs -rmr ${WEBLOG_COMMMON_MIDLAYER_HDFS_DIR}originalContentAll/$delateday
${HADOOP} fs -rmr ${WEBLOG_COMMMON_MIDLAYER_HDFS_DIR}articleAll_web/$delateday
${HADOOP} fs -rmr ${WEBLOG_COMMMON_MIDLAYER_HDFS_DIR}dailyUrlInfoVector/$delateday
${HADOOP} fs -rmr ${WEBLOG_COMMMON_MIDLAYER_HDFS_DIR}dailyArticleSource/$delateday
${HADOOP} fs -rmr ${WEBLOG_COMMMON_MIDLAYER_HDFS_DIR}dailyUrlGenTieCountAndUserCount/$delateday
${HADOOP} fs -rmr ${WEBLOG_COMMMON_MIDLAYER_HDFS_DIR}dailyUrlGenTieCountAndUserCountTemp/$delateday
${HADOOP} fs -rmr ${WEBLOG_COMMMON_MIDLAYER_HDFS_DIR}dailyUrlOriginalContent/$delateday
${HADOOP} fs -rmr ${WEBLOG_COMMMON_MIDLAYER_HDFS_DIR}dailyUrlPvUvShareBack/$delateday
${HADOOP} fs -rmr ${WEBLOG_COMMMON_MIDLAYER_HDFS_DIR}dailyUrlPvUvShareBackTemp/$delateday
${HADOOP} fs -rmr ${WEBLOG_COMMMON_MIDLAYER_HDFS_DIR}devilfishToMap/$delateday
${HADOOP} fs -rmr ${WEBLOG_COMMMON_MIDLAYER_HDFS_DIR}weblogToMap/$delateday
${HADOOP} fs -rmr ${WEBLOG_COMMMON_HDFS_DIR}devilfishLog/$delateday
${HADOOP} fs -rmr ${WEBLOG_COMMMON_HDFS_DIR}weblog/$delateday
${HADOOP} fs -rmr ${WEBLOG_COMMMON_HDFS_DIR}docidAndUrlOfRarticle/$bigdataHouseDelateday

