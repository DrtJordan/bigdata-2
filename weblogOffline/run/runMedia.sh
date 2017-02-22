#!/bin/bash
baseDir=$(cd "$(dirname "$0")"; pwd)
source $baseDir/weblogHeader.sh
 function main() {	      
	
	    beforeRunMRMoudle MediaLogMR   	 
	    sh $baseDir/runMRJob.sh -c com.netease.weblogOffline.common.UrlMediaMergeMR -d $yesterday
	    sh $baseDir/media/mediaDaily.sh $yesterday
		csMediaStatus=$?
		echo csMediaStatus=$csMediaStatus

		afterRunMRMoudle MediaLogMR 3 600 
		 
		if [ "$errorList" != "" ];then
			errorAlarm MediaLogMR:$errorList
		fi
}
main
