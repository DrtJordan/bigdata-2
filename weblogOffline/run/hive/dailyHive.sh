#!/bin/bash

if [ $# = 0 ]; then
    yesterday=`date +%Y%m%d -d "-1days"`
else
    yesterday=$1
fi

export JAVA_HOME=${JAVA_HOME}
export HADOOP_HOME=${HADOOP_HOME}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}

echo $yesterday


listDataDest=${WEBLOG_RESULT_LOCAL_DIR}list
if [ ! -d $listDataDest ];then
    mkdir -p $listDataDest
fi

hql="select event,ptype,pver,count(uuid),count(distinct uuid) from weblog where dt=$yesterday and (url=\"http://www.163.com/\" or url like \"http://www.163.com/#%\") and (event like \"launch%\" or event like \"initializ%\") and ((ptype=\"ab_0\" and pver=\"b\") or (ptype=\"ab_1\" and pver=\"c\") or (ptype=\"ab_2\" and pver=\"d\") or (ptype=\"ab_3\" and pver=\"e\") or (ptype=\"ab_4\" and pver=\"f\") or (ptype=\"\" and pver=\"\") or (ptype=\"\" and pver=\"a\") ) group by event,ptype,pver;"
/bin/bash /home/weblog/.weblogHive $hql | sort -k 4 -nr | awk '{print "'$yesterday'",$0}'> $dest/homeABTest$yesterday
/bin/bash /home/weblog/.weblogHive $hql | sort -k 4 -nr | awk '{print $0}'> $listDataDest/homeABTest$yesterday

#toDcList
/usr/bin/rsync -au $listDataDest/ 123.58.179.99::dcList/pro6/




