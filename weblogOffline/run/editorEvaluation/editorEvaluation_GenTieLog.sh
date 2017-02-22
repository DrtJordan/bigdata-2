#!/bin/bash

baseDir=$(cd "$(dirname "$0")"; pwd)

source $baseDir/../weblogHeader.sh

beforeRunMRMoudle editorEvaluation_GenTieLog

sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.editorEvaluation.GeneralizedPvUvMR -d $yesterday \
	 -input /ntes_weblog/commonData/genTieLogNew/$yesterday \
	 -output /ntes_weblog/weblog/statistics/editorEvaluation/genTie/$yesterday/step_1/,/ntes_weblog/weblog/statistics/editorEvaluation/genTie/$yesterday/step_2/,/ntes_weblog/weblog/statistics/editorEvaluation/genTie/$yesterday/res/ \
	 --pvUvArgsGenerator com.netease.weblogOffline.statistics.editorEvaluation.generators.GenTiePvUvArgsGenerator 

afterRunMRMoudle editorEvaluation_GenTieLog 3 600 

if [ "$errorList" != "" ];then
	errorAlarm editorEvaluation_GenTieLog:$errorList
fi





 








