#!/bin/bash

baseDir=$(cd "$(dirname "$0")"; pwd)

source $baseDir/../weblogHeader.sh

beforeRunMRMoudle editorEvaluation_Combine

sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.editorEvaluation.combine.BaseInfoAnd3gCombineMRTemp -d $yesterday
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.editorEvaluation.combine.GenTieUpDownInfoCombineMR -d $yesterday

sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.editorEvaluation.combine.PcInfoCombineMR -d $yesterday
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.editorEvaluation.combine.AppInfoCombineMR -d $yesterday

sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.editorEvaluation.combine.WapInfoCombineTempMR -d $yesterday
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.editorEvaluation.combine.WapInfoAndShareCombineMR -d $yesterday

sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.editorEvaluation.combine.PcAppWapInfoCombineMR -d $yesterday
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.editorEvaluation.combine.GenTieAndPcAppWapInfoCombineMR -d $yesterday

sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.editorEvaluation.combine.ContentInfoAllMR -d $yesterday
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.editorEvaluation.combine.BaseInfoAnd3gIncrMR -d $yesterday

sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.editorEvaluation.combine.InfoGroupByPICMR  -d $yesterday

afterRunMRMoudle editorEvaluation_Combine 3 600 

if [ "$errorList" != "" ];then
	errorAlarm editorEvaluation_Combine:$errorList
fi





 








