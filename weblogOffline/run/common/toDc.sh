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
listDataDest=${WEBLOG_RESULT_LOCAL_DIR}list

if [ ! -d $listDataDest ];then
    mkdir -p $listDataDest
fi

echo $yesterday
function main {
#toDc
${HADOOP} jar ${JAR} com.netease.weblogOffline.common.DataExporter /ntes_weblog/weblog/statistics/result_toDC/ $yesterday http://datacube.ws.netease.com/expdata/importData.do
#${HADOOP} jar ${JAR} com.netease.weblogOffline.common.DataExporter /ntes_weblog/weblog/statistics/result_toDC/ $yesterday http://buzz100x.hz.youdao.com:30088/expdata/importData.do

#toDcList
/usr/bin/rsync -au $listDataDest/ 123.58.179.99::dcList/pro6/
/usr/bin/rsync -au $listDataDest/ 10.130.10.96::dcList/pro6/
}
main
