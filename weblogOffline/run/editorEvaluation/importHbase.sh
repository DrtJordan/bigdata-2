#!/bin/bash

baseDir=$(cd "$(dirname "$0")"; pwd)

source $baseDir/../weblogHeader.sh

#URL粒度统计
${HADOOP} jar ${JAR} com.netease.weblogOffline.statistics.editorEvaluation.hbase.ImportHbase4URL /ntes_weblog/weblog/statistics/editorEvaluation/genTieAndPcAppWapInfoCombine/ $yesterday
${HADOOP} jar ${JAR} com.netease.weblogOffline.statistics.editorEvaluation.hbase.BuildIndex /ntes_weblog/weblog/statistics/editorEvaluation/baseInfoAnd3gIncr/ $yesterday

#小编粒度统计
${HADOOP} jar ${JAR} com.netease.weblogOffline.statistics.editorEvaluation.hbase.ImportHbase4Editor /ntes_weblog/weblog/statistics/editorEvaluation/infoGroupByPIC/ $yesterday

exit 0




 








