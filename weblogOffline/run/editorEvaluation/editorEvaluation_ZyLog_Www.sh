#!/bin/bash

baseDir=$(cd "$(dirname "$0")"; pwd)

source $baseDir/../weblogHeader.sh

beforeRunMRMoudle editorEvaluation_ZyLog
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.editorEvaluation.needColumn.NeedColumnZyLogMR -d $yesterday \
     -input /ntes_weblog/commonData/devilfishLogClassification/devilfishLog_www/$yesterday \
     -output /ntes_weblog/weblog/statistics/editorEvaluation/pvUv_Session_Back_NeedColumnFromZy/$yesterday
#pvuv back
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.editorEvaluation.GeneralizedPvUvMR -d $yesterday \
	 -input /ntes_weblog/weblog/statistics/editorEvaluation/pvUv_Session_Back_NeedColumnFromZy/$yesterday \
	 -output /ntes_weblog/weblog/statistics/editorEvaluation/pvUvAndBack_www/$yesterday/step_1/,/ntes_weblog/weblog/statistics/editorEvaluation/pvUvAndBack_www/$yesterday/step_2/,/ntes_weblog/weblog/statistics/editorEvaluation/pvUvAndBack_www/$yesterday/res/ \
	 --pvUvArgsGenerator com.netease.weblogOffline.statistics.editorEvaluation.generators.AllPvUvAndBackPvUvArgsGeneratorWww 

#share
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.editorEvaluation.GeneralizedPvUvMR -d $yesterday \
	 -input /ntes_weblog/commonData/devilfishLogClassification/shareLog_www/$yesterday \
	 -output /ntes_weblog/weblog/statistics/editorEvaluation/share_www/$yesterday/step_1/,/ntes_weblog/weblog/statistics/editorEvaluation/share_www/$yesterday/step_2/,/ntes_weblog/weblog/statistics/editorEvaluation/share_www/$yesterday/res/ \
	 --pvUvArgsGenerator com.netease.weblogOffline.statistics.editorEvaluation.generators.SharePvUvArgsGeneratorWww 

#session 
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.editorEvaluation.GeneralizedPvUvMR -d $yesterday \
	 -input /ntes_weblog/weblog/statistics/editorEvaluation/pvUv_Session_Back_NeedColumnFromZy/$yesterday \
	 -output /ntes_weblog/weblog/statistics/editorEvaluation/sessionCount_www/$yesterday/step_1/,/ntes_weblog/weblog/statistics/editorEvaluation/sessionCount_www/$yesterday/step_2/,/ntes_weblog/weblog/statistics/editorEvaluation/sessionCount_www/$yesterday/res/ \
	 --pvUvArgsGenerator com.netease.weblogOffline.statistics.editorEvaluation.generators.SessionCountArgsGeneratorWww 

afterRunMRMoudle editorEvaluation_ZyLog 3 600 

if [ "$errorList" != "" ];then
	errorAlarm editorEvaluation_ZyLog:$errorList
fi





 








