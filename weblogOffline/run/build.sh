#/sbin/bash

#if [ $# == 1 ]; then
#	branch=$1
#else
#	branch=master
#fi
installTime=`date +%Y%m%d-%H:%M`

basedir=$(cd "$(dirname "$0")"; pwd)
echo $basedir
cd ..

#mvn clean
#git pull https://git.ws.netease.com/cms-analyzer/bigdata.git $branch
#cp filter.properties.online filter.properties
#mvn package

cd ../../../

installDir=./release/$installTime
if [ -d $installDir ];then
	rm -r $installDir
fi
mkdir -p $installDir

cp $basedir/../target/*.jar $installDir
cp -r $basedir/../target/classes/* $installDir

runDir=./webAnalysis
if [ -h $runDir ];then
	rm $runDir
fi

ln -s $installDir $runDir



