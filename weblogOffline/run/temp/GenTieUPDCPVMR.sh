#!/bin/bash

if [ $# = 2 ]; then
    from=$1
    to=$2
else
	echo "error date"
fi

date=$from
end=`date +%Y%m%d -d "$to+1days"`
while [ "$date" != "$end" ];
do
    echo $date
	/home/weblog/hadoop/bin/hadoop jar /home/weblog/webAnalysis/webAnalysis/weblogOffline-0.1.0-jar-with-dependencies.jar com.netease.jurassic.hadoopjob.RunMRJob -force -c com.netease.weblogOffline.temp.GenTieUCPDPUMR -d $date 2>&1    
    date=`date +%Y%m%d -d "$date+1days"`
done







 







