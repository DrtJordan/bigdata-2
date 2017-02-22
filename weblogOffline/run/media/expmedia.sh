#!/bin/bash

if [ $# = 0 ]; then
    yesterday=`date +%Y%m%d -d "-1days"`
else
    yesterday=$1
fi


echo $yesterday

baseDir=$(cd "$(dirname "$0")"; pwd)
source $baseDir/../weblogHeader.sh

hadoopSourceDir=/ntes_weblog/media/statistics/mediaexpfile
localtempfile=${LOCAL_BASE_DIR}/mediaFile


##导入媒体数据转换格式
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.exp.MediaExpFileMR  -d $yesterday

##媒体数据下载到本地 ${LOCAL_BASE_DIR}/mediaFile
if [ ! -d $localtempfile ];then
	mkdir -p $localtempfile
fi

if [ -f $localtempfile/pc_$yesterday ];then
	rm  $localtempfile/pc_$yesterday
fi
${HADOOP} fs -get $hadoopSourceDir/$yesterday/p* $localtempfile/pc_$yesterday
chmod +r $localtempfile/pc_$yesterday

##推送到发布器
/usr/bin/rsync -au $localtempfile/pc_$yesterday 106.38.231.28::mediacountdata

