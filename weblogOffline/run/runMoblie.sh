#!/bin/bash
baseDir=$(cd "$(dirname "$0")"; pwd)
source $baseDir/weblogHeader.sh
 function main() {	      
	    #移动数据 相关业务 
	     beforeRunMRMoudle MobileLogMR   	 
    	 sh $baseDir/common/getMobileLogAndCheck.sh  $yesterday
	     if [ $? = 1 ];then
	     exit 1
	     fi
	     #增加移动数据
		sh $baseDir/contentScore/csMobile.sh $yesterday
		csMobileStatus=$?
		echo csMobileStatus=$csMobileStatus

		afterRunMRMoudle MobileLogMR 3 600 
		 
		if [ "$errorList" != "" ];then
			errorAlarm MobileLogMR:$errorList
		fi
}
main
