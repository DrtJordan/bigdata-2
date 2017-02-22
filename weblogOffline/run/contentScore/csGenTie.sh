#!/bin/bash
# sh getGenTie.sh 20141205

baseDir=$(cd "$(dirname "$0")"; pwd)

source $baseDir/../weblogHeader.sh


function main(){

	beforeRunMRMoudle csGenTie
	
	sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.contentscore.DailyGenTieCountMR -d $yesterday
	sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.contentscore.GenTieChannelSpecialUvMR -d $yesterday
	
	afterRunMRMoudle csGenTie 3 600
	
	if [ "$errorList" != "" ];then
		errorAlarm csGenTie:$errorList
		exit 1
	else
		exit 0
	fi
}

main


 







