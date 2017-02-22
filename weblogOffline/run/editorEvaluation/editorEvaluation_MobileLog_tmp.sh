#!/bin/sh


baseDir=$(cd "$(dirname "$0")"; pwd)

cd $baseDir

if [ $# == 2 ]; then
    yesterday=$1
elif [ $# == 1 ]; then
	yesterday=$1
else
	yesterday=`date +%Y%m%d -d "-1days"`
fi

source $baseDir/../weblogHeader.sh
beforeRunMRMoudle editorEvaluation_MobileLog
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.editorEvaluation.MobileLogParseMR -d $yesterday

sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.editorEvaluation.GeneralizedPvUvMR -d $yesterday -notCheckInputFile \
	 -input /ntes_weblog/weblog/statistics/editorEvaluation/mobileHiveLog/$yesterday \
	 -output /ntes_weblog/weblog/statistics/editorEvaluation/pvUvAndShare_app/$yesterday/step_1_tmp/,/ntes_weblog/weblog/statistics/editorEvaluation/pvUvAndShare_app/$yesterday/step_2_tmp/,/ntes_weblog/weblog/statistics/editorEvaluation/pvUvAndShare_app/$yesterday/res_tmp/ \
	 --pvUvArgsGenerator com.netease.weblogOffline.statistics.editorEvaluation.generators.PvUvAndShareGeneratorApp 

afterRunMRMoudle editorEvaluation_MobileLog 3 600 

if [ "$errorList" != "" ];then
	errorAlarm editorEvaluation_MobileLog:$errorList
fi





