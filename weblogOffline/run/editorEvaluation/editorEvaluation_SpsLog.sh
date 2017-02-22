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
beforeRunMRMoudle editorEvaluation_SpsLog
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.editorEvaluation.GeneralizedPvUvMR -d $yesterday \
	 -input /ntes_weblog/commonData/devilfishLogClassification/shareBackLog_sps/$yesterday \
	 -output /ntes_weblog/weblog/statistics/editorEvaluation/spsShareBack/$yesterday/step_1/,/ntes_weblog/weblog/statistics/editorEvaluation/spsShareBack/$yesterday/step_2/,/ntes_weblog/weblog/statistics/editorEvaluation/spsShareBack/$yesterday/res/ \
	 --pvUvArgsGenerator com.netease.weblogOffline.statistics.editorEvaluation.generators.ShareBackGeneratorWap


afterRunMRMoudle editorEvaluation_SpsLog 3 600 

if [ "$errorList" != "" ];then
	errorAlarm editorEvaluation_SpsLog:$errorList
fi





