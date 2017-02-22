#!/bin/sh

baseDir=$(cd "$(dirname "$0")"; pwd)

hadoop_cms_cache_dir=${WEBLOG_CACHE_HDFS_DIR}cmsdata

${HADOOP} fs -test -e $hadoop_cms_cache_dir
if [ $? -eq 0 ] ;then
  ${HADOOP} fs -rmr $hadoop_cms_cache_dir/*
else
  ${HADOOP} fs -mkdir $hadoop_cms_cache_dir
fi

cd $baseDir/../conf/
${HADOOP} fs -put cmsdata/* $hadoop_cms_cache_dir
