#!/bin/bash
# sh csMobile.sh 20141205

baseDir=$(cd "$(dirname "$0")"; pwd)

source $baseDir/../weblogHeader.sh
localDestDir=${WEBLOG_RESULT_LOCAL_DIR}weblogRsync/mobile/$yesterday
jiguangDataDest=${WEBLOG_RESULT_LOCAL_DIR}toJiGuang/$yesterday

if [ ! -d $jiguangDataDest ];then
    mkdir -p $jiguangDataDest
fi

function main(){
	
	sh $baseDir/../runCommand.sh com.netease.weblogOffline.statistics.contentscore.MergePcMobile $jiguangDataDest/${yesterday}_ycDetail.txt $localDestDir/mobi_${yesterday} $jiguangDataDest/${yesterday}_ycDetail_all.txt
	#toJiGuang
	/usr/bin/rsync -au $jiguangDataDest/${yesterday}_ycDetail_all.txt 223.252.196.104::kpi
}

main


 








 







