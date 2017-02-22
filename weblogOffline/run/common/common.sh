#!/bin/bash
if [ $# = 0 ]; then
    yesterday=`date +%Y%m%d -d "-1days"`
else
    yesterday=$1
fi
export JAVA_HOME=${JAVA_HOME}
export HADOOP_HOME=${HADOOP_HOME}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}
baseDir=$(cd "$(dirname "$0")"; pwd)
source $baseDir/../weblogHeader.sh
echo $yesterday
status=0
function main {
	#get and check  data
	echo $model
	if  [ "$model" = online ];then
	    sh $baseDir/weblogCheck.sh $yesterday
	    if [ $? = 1 ];then
	    	status=1
	    fi
	    	
		sh $baseDir/zyCheck.sh  $yesterday
	    if [ $? = 1 ];then
	    	status=1
	    fi
	    
#	    sh $baseDir/getGenTieLogAndCheck.sh  $yesterday
#	    if [ $? = 1 ];then
#	    	status=1
#	    fi
	    
	    sh $baseDir/getGenTieLogNewAndCheck.sh  $yesterday
#	    if [ $? = 1 ];then
#	    	status=1
#	    fi
	    
	    sh $baseDir/getOriginalContentLogAndCheck.sh  $yesterday
	    if [ $? = 1 ];then
	    	status=1
	    fi  
	        
	    sh $baseDir/get3gLogAndCheck.sh $yesterday
	    if [ $? = 1 ];then
	    	status=1
	    fi 
	    
	    sh $baseDir/getGenTieBaseAndCheck.sh  $yesterday
	    if [ $? = 1 ];then
	    	status=1
	    fi
	    
	     sh $baseDir/getUrlMediaLogAndCheck.sh  $yesterday
	     if [ $? = 1 ];then
	         status=1
	     fi
	    
	    sh $baseDir/getGenTieVoteLogAndCheck.sh $yesterday
#	    if [ $? = 1 ];then
#	    	status=1
#	    fi  

        sh $baseDir/getPhotoSetAndCheck.sh  $yesterday
#	    if [ $? = 1 ];then
#	    	status=1
#	    fi  
        
        sh $baseDir/getArticleAndCheck.sh  $yesterday
#	    if [ $? = 1 ];then
#	    	status=1
#	    fi  
        sh $baseDir/getSpecialAndCheck.sh  $yesterday
#	    if [ $? = 1 ];then
#	    	status=1
#	    fi 
        sh $baseDir/getDyDataAndCheck.sh  $yesterday
#	    if [ $? = 1 ];then
#	    	status=1
#	    fi
        sh $baseDir/getVidioAndCheck.sh  $yesterday
#	    if [ $? = 1 ];then
#	    	status=1
#	    fi  
        sh $baseDir/getEditor_3gAndCheck.sh  $yesterday
#	    if [ $? = 1 ];then
#	    	status=1
#	    fi  
        if [ $status = 1 ];then
            exit 1
        fi    
	fi
	  
    beforeRunMRMoudle commonMR
		sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.common.weblogfilter.WebLogFilter  -d $yesterday
		sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.common.zylogfilter.ZyLogFilter  -d $yesterday
		sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.common.GenTieBaseMergeMR -d $yesterday
		sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.common.Content_3gMergeMR_new -d $yesterday
		sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.common.PhotoSetMergeMR -d $yesterday
		sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.common.ArticleMergeMR -d $yesterday
		sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.common.DocidAndUrlOfArticleMR -d $yesterday
		sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.common.SpecialMergeMR -d $yesterday
		sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.common.VidioMergeMR -d $yesterday
		
	afterRunMRMoudle commonMR 3 600 
	  if [ "$errorList" != "" ];then
			errorAlarm commonMR:$errorList
			exit 1
	  else
            exit 0 
	  fi
}
main
