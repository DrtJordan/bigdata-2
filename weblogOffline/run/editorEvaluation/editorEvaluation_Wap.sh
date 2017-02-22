#!/bin/bash

baseDir=$(cd "$(dirname "$0")"; pwd)

source $baseDir/../weblogHeader.sh

beforeRunMRMoudle editorEvaluation_Wap
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.editorEvaluation.needColumn.NeedColumnZyLogMR -d $yesterday \
     -input /ntes_weblog/commonData/devilfishLogClassification/wap/$yesterday \
     -output /ntes_weblog/weblog/statistics/editorEvaluation/needColumnFormZy4Wap/$yesterday

#pvuv
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.editorEvaluation.GeneralizedPvUvMR -d $yesterday \
     -input /ntes_weblog/weblog/statistics/editorEvaluation/needColumnFormZy4Wap/$yesterday \
     -output /ntes_weblog/weblog/statistics/editorEvaluation/pvUv_wap/$yesterday/step_1/,/ntes_weblog/weblog/statistics/editorEvaluation/pvUv_wap/$yesterday/step_2/,/ntes_weblog/weblog/statistics/editorEvaluation/pvUv_wap/$yesterday/res/ \
     --pvUvArgsGenerator com.netease.weblogOffline.statistics.editorEvaluation.generators.PvUvArgsGeneratorWap 

#session 
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.editorEvaluation.GeneralizedPvUvMR -d $yesterday \
     -input /ntes_weblog/weblog/statistics/editorEvaluation/needColumnFormZy4Wap/$yesterday \
     -output /ntes_weblog/weblog/statistics/editorEvaluation/sessionCount_wap/$yesterday/step_1/,/ntes_weblog/weblog/statistics/editorEvaluation/sessionCount_wap/$yesterday/step_2/,/ntes_weblog/weblog/statistics/editorEvaluation/sessionCount_wap/$yesterday/res/ \
     --pvUvArgsGenerator com.netease.weblogOffline.statistics.editorEvaluation.generators.SessionCountArgsGeneratorWap

afterRunMRMoudle editorEvaluation_Wap 3 600 

if [ "$errorList" != "" ];then
    errorAlarm editorEvaluation_Wap:$errorList
fi

