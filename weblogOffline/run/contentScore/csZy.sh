#!/bin/bash
# sh cs_zy.sh 20141205

baseDir=$(cd "$(dirname "$0")"; pwd)

source $baseDir/../weblogHeader.sh


function main(){
	
	
	beforeRunMRMoudle csZy
	
	sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.contentscore.ShareBackCountMR -d $yesterday
	sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.contentscore.ContentPvUvMR -d $yesterday
	sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.contentscore.ChannelAndHomePvUvFromZyMR -d $yesterday
	
	afterRunMRMoudle csZy 3 600
	
	if [ "$errorList" != "" ];then
		errorAlarm csZy:$errorList
		exit 1
	else
		exit 0
	fi
}

main






 







