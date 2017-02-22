#!/bin/bash
# sh getYcInfo.sh 20141205

baseDir=$(cd "$(dirname "$0")"; pwd)

source $baseDir/../weblogHeader.sh


function main(){
	
	beforeRunMRMoudle csYcInfo
	
	sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.contentscore.DailyYcInfoMergeMR -d $yesterday

	afterRunMRMoudle csYcInfo 3 600
	
	if [ "$errorList" != "" ];then
		errorAlarm csYuanChuangInfo:$errorList
		exit 1
	else
		exit 0
	fi
}

main


 








 







