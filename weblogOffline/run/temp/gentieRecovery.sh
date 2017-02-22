#!/bin/bash

export JAVA_HOME=${JAVA_HOME}
export HADOOP_HOME=${HADOOP_HOME}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}
baseDir=$(cd "$(dirname "$0")"; pwd)

gentie=/home/weblog/gaojinnan/gentie

gentieNew=/home/weblog/gaojinnan/gentieNew

function copyToNewFileName(){
    
	for file in `ls $gentie `;do
	        cat $file |awk -F',' '{print $0",null,null,null,null"}' > $gentieNew/$file
	done
}


function main(){
	copyToNewFileName	
}

main
