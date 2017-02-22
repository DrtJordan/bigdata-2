#!/bin/bash

baseDir=$(cd "$(dirname "$0")"; pwd)

source $baseDir/../../weblogHeader.sh

beforeRunMRMoudle editorEvaluation_Main


##--MobileLog------------------------
sh $baseDir/../editorEvaluation_MobileLog.sh $yesterday
editorEvaluation_MobileLogStatus=$?
echo editorEvaluation_MobileLogStatus=$editorEvaluation_MobileLogStatus

sh $baseDir/../../runMRJob.sh -c com.netease.weblogOffline.statistics.editorEvaluation.combine.AppInfoCombineMR -d $yesterday
sh $baseDir/../../runMRJob.sh -c com.netease.weblogOffline.statistics.editorEvaluation.combine.AllInfoCombineMR -d $yesterday
sh $baseDir/../../runMRJob.sh -c com.netease.weblogOffline.statistics.editorEvaluation.combine.GenTieAndAllInfoCombineMR -d $yesterday
sh $baseDir/../../runMRJob.sh -c com.netease.weblogOffline.statistics.editorEvaluation.combine.ContentInfoAllMR -d $yesterday
sh $baseDir/../../runMRJob.sh -c com.netease.weblogOffline.statistics.editorEvaluation.combine.BaseInfoAnd3gIncrMR -d $yesterday
sh $baseDir/../../runMRJob.sh -c com.netease.weblogOffline.statistics.editorEvaluation.combine.InfoGroupByPICMR  -d $yesterday

sh $baseDir/../importHbase.sh $yesterday
importHbaseStatus=$?
echo importHbaseStatus=$importHbaseStatus

afterRunMRMoudle editorEvaluation_Main 3 600 
if [ "$errorList" != "" ];then
	errorAlarm editorEvaluation_Main:$errorList
fi





 








