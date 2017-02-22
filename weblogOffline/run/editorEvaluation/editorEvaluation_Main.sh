#!/bin/bash

baseDir=$(cd "$(dirname "$0")"; pwd)

source $baseDir/../weblogHeader.sh

beforeRunMRMoudle editorEvaluation_Main
##--zylog------------------------
sh $baseDir/editorEvaluation_ZyLog_Www.sh $yesterday
editorEvaluation_ZyLogStatus=$?
echo editorEvaluation_ZyLogStatus=$editorEvaluation_ZyLogStatus

##--GenTie顶踩------------------------
sh $baseDir/editorEvaluation_GenTieUpDown_All.sh $yesterday
editorEvaluation_GenTieUpDownStatus=$?
echo editorEvaluation_GenTieUpDownStatus=$editorEvaluation_GenTieUpDownStatus

##--GenTieLog------------------------
sh $baseDir/editorEvaluation_GenTieLog.sh $yesterday
editorEvaluation_GenTieLogStatus=$?
echo editorEvaluation_GenTieLogStatus=$editorEvaluation_GenTieLogStatus

##--MobileLog------------------------
sh $baseDir/editorEvaluation_MobileLog.sh $yesterday
editorEvaluation_MobileLogStatus=$?
echo editorEvaluation_MobileLogStatus=$editorEvaluation_MobileLogStatus

##--SpsLog------------------------
sh $baseDir/editorEvaluation_SpsLog.sh $yesterday
editorEvaluation_SpsLogStatus=$?
echo editorEvaluation_SpsLogStatus=$editorEvaluation_SpsLogStatus

##--WapLog------------------------
sh $baseDir/editorEvaluation_Wap.sh $yesterday
editorEvaluation_WapLogStatus=$?
echo editorEvaluation_WapLogStatus=$editorEvaluation_WapLogStatus

if [ $editorEvaluation_ZyLogStatus -ne 0 -a $editorEvaluation_GenTieUpDownStatus -ne 0 -a $editorEvaluation_GenTieLogStatus -ne 0 -a $editorEvaluation_MobileLogStatus -ne 0 -a $editorEvaluation_SpsLogStatus -ne 0 -a editorEvaluation_WapLogStatus -ne 0 ];then
	errorAlarm editorEvaluation_ZyLogStatus=${editorEvaluation_ZyLogStatus},editorEvaluation_GenTieUpDownStatus=${editorEvaluation_GenTieUpDownStatus},editorEvaluation_GenTieLogStatus=${editorEvaluation_GenTieLogStatus},editorEvaluation_MobileLogStatus=${editorEvaluation_MobileLogStatus},editorEvaluation_SpsLogStatus=$editorEvaluation_SpsLogStatus
	exit 1
fi
##--结果合并------------------------
sh $baseDir/editorEvaluation_Combine.sh $yesterday
editorEvaluation_CombineStatus=$?
echo editorEvaluation_CombineStatus=$editorEvaluation_CombineStatus

sh $baseDir/importHbase.sh $yesterday
importHbaseStatus=$?
echo importHbaseStatus=$importHbaseStatus

afterRunMRMoudle editorEvaluation_Main 3 600 
if [ "$errorList" != "" ];then
	errorAlarm editorEvaluation_Main:$errorList
fi





 








