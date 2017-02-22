#!/bin/bash
baseDir=$(cd "$(dirname "$0")"; pwd)
source $baseDir/weblogHeader.sh

function main() {
	##--基础数据准备------------------------
	sh $baseDir/common/common.sh $yesterday $model
	if [ $? = 1 ];then
	   exit 1
	fi
	
	##--数据仓库------------------------
	sh $baseDir/bigdatahouse/bigdataHouse.sh $yesterday
	
	##--频道考核------------------------
	sh $baseDir/contentScore/csMain.sh $yesterday
	
	##--需要导入dc------------------------
	sh $baseDir/dailyWeblog.sh $yesterday
	sh $baseDir/gentieinfo/gentieInfo.sh $yesterday
	sh $baseDir/tokaola/kaola.sh $yesterday
	sh $baseDir/changechannel/ChangeChannel.sh $yesterday
	sh $baseDir/runMedia.sh  $yesterday
	sh $baseDir/cms/cmsDaily.sh $yesterday
	
	##--导入dc------------------------
	sh $baseDir/common/toDc.sh $yesterday
	##--其它------------------------
	sh $baseDir/runMoblie.sh  $yesterday
}
main
