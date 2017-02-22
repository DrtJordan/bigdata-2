#!/bin/bash

baseDir=$(cd "$(dirname "$0")"; pwd)

source $baseDir/../weblogHeader.sh

beforeRunMRMoudle editorEvaluation_GenTieUpDown

# updown
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.editorEvaluation.needColumn.GentieUpDownNeedColumnMR -d $yesterday

sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.editorEvaluation.GeneralizedPvUvMR -d $yesterday \
	 -input /ntes_weblog/weblog/statistics/editorEvaluation/gentieUpDownNeedColumn/$yesterday \
	 -output /ntes_weblog/weblog/statistics/editorEvaluation/articleUpDown_www/$yesterday/step_1/,/ntes_weblog/weblog/statistics/editorEvaluation/articleUpDown_www/$yesterday/step_2/,/ntes_weblog/weblog/statistics/editorEvaluation/articleUpDown_www/$yesterday/res/ \
	 --pvUvArgsGenerator com.netease.weblogOffline.statistics.editorEvaluation.generators.ContentUpDownArgsGeneratorAll 

sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.editorEvaluation.GeneralizedPvUvMR -d $yesterday \
	 -input /ntes_weblog/weblog/statistics/editorEvaluation/gentieUpDownNeedColumn/$yesterday \
	 -output /ntes_weblog/weblog/statistics/editorEvaluation/gentieUpDown_www/$yesterday/step_1/,/ntes_weblog/weblog/statistics/editorEvaluation/gentieUpDown_www/$yesterday/step_2/,/ntes_weblog/weblog/statistics/editorEvaluation/gentieUpDown_www/$yesterday/res/ \
	 --pvUvArgsGenerator com.netease.weblogOffline.statistics.editorEvaluation.generators.GenTieUpDownArgsGeneratorAll 

afterRunMRMoudle editorEvaluation_GenTieUpDown 3 600 

if [ "$errorList" != "" ];then
	errorAlarm editorEvaluation_GenTieUpDown:$errorList
fi





 








