#!/bin/bash

export JAVA_HOME=${JAVA_HOME}
export HADOOP_HOME=${HADOOP_HOME}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}
baseDir=$(cd "$(dirname "$0")"; pwd)

devilfishLog_temp_HadoopDestDir=/ntes_weblog/devilfishTemp



function copyToNewFileName(){

    for channel in `${HADOOP} fs -ls /ntes_weblog/devilfishTemp/10.120.116.233 |grep -v "Found"|awk -F'/' '{print $5}'`;do       
                 for date in `${HADOOP} fs -ls /ntes_weblog/devilfishTemp/10.120.116.233/$channel |grep -v "Found"|awk -F'/' '{print $6}'`;do
                      for file in `${HADOOP} fs -ls /ntes_weblog/devilfishTemp/10.120.116.233/$channel/$date |grep -v "Found"|awk -F'/' '{print $7}'`;do
                       ${HADOOP} fs -cp /ntes_weblog/devilfishTemp/$ip/$channel/$date/$file ${ZYLOG_DIR}/$date/10.120.116.233_$file
                       echo /ntes_weblog/devilfishTemp/10.120.116.233/$channel/$date
                       done
                 done        
          
    done
}


function main(){
    copyToNewFileName    
}

main