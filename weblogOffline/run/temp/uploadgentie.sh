#!/bin/bash

if [ $# = 0 ]; then
    
else
    yesterday=$1
    local=`date +%Y-%m-%d -d "$yesterday"`
fi
export JAVA_HOME=${JAVA_HOME}
export HADOOP_HOME=${HADOOP_HOME}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}
baseDir=$(cd "$(dirname "$0")"; pwd)

gentie=/home/weblog/gaojinnan/gentieNew

gentieNew=

function uploadNewFileName(){
    
	 hadoop fs -put /home/weblog/gaojinnan/gentieNew/*${local}* /ntes_weblog/commonData/genTieLogNew/yesterday
}


function main(){
	uploadNewFileName	
}

main
