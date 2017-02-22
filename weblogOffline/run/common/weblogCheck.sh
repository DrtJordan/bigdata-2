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

weblogStatus="fault"
weblogHours=0
function check(){
	weblogHours=`${HADOOP} fs -du ${WEBLOG_DIR}/$yesterday/ | grep $yesterday | awk -F'-' '{print substr($(NF-1),1,10)}' | sort -u | wc -l`
	if [ $weblogHours -eq 24 ];then
	    weblogStatus="success"
	fi
}

function main(){
	check	
	echo "weblogCheck"
	if [ "$weblogStatus" == "fault" ];then
		errorAlarm weblogCheck:weblogHours=$weblogHours/24
		exit 1
	else
		exit 0
	fi
}

main
