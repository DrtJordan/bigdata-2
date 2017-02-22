#!/bin/bash

export JAVA_HOME=${JAVA_HOME}
export HADOOP_HOME=${HADOOP_HOME}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}
baseDir=$(cd "$(dirname "$0")"; pwd)

devilfishLog_temp_HadoopDestDir=/ntes_weblog/devilfishTemp



function copyToNewFileName(){
    
	for ip in `${HADOOP} fs -ls /ntes_weblog/devilfishTemp |grep -v "Found"|awk -F'/' '{print $4}'`;do
	        for channel in `${HADOOP} fs -ls /ntes_weblog/devilfishTemp/$ip |grep -v "Found"|awk -F'/' '{print $5}'`;do
	           
	                  for file in `${HADOOP} fs -ls /ntes_weblog/devilfishTemp/$ip/$channel/20160108 |grep -v "Found"|awk -F'/' '{print $7}'`;do
	                   ${HADOOP} fs -cp /ntes_weblog/devilfishTemp/$ip/$channel/20160108/$file ${ZYLOG_DIR}/20160108/${ip}_$file
	                   echo /ntes_weblog/devilfishTemp/$ip/$channel/20160108
	                   done
	           
	        done   
	done
}


function main(){
	copyToNewFileName	
}

main