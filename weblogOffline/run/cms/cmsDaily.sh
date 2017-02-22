#!/bin/bash

baseDir=$(cd "$(dirname "$0")"; pwd)
source $baseDir/../weblogHeader.sh

echo $yesterday

beforeRunMRMoudle CMS

# CMS各频道分类PV、UV
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.cms.ChannelClassifyPvUvMR  -d $yesterday

# CMS各频道跟帖、分享、回流
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.cms.ChannelTieShareBackMR  -d $yesterday

afterRunMRMoudle CMS 3 600 

if [ "$errorList" != "" ];then
    errorAlarm CMS:$errorList
fi  




