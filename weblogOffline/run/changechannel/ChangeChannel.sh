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

#计算各个频道 在各个版本下文章页的pvuv
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.changechannel.ChannelofBroadPvUvFromWeblogMR  -d $yesterday

#计算各个频道在各个版本下文章页 有Email的用户数 和没Email的用户数
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.changechannel.EmailOrNoEmailUvFromWeblogMR  -d $yesterday


# 计算各个频道在各个版本下文章页对应url的pv
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.changechannel.ChannelBroadurlPvMR  -d $yesterday

# 计算各个频道在各个版本下文章页对应url的sidcount
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.changechannel.ChannelBroadurlSIDCountMR  -d $yesterday


# 计算各个频道在各个版本下click事件文章页对应的url
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.changechannel.ChannelClickUrlMR  -d $yesterday
# 计算各个频道在各个版本下click事件总和 以及 涉及url pv
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.changechannel.ChannelClickUrlCountAndUrlPv  -d $yesterday
# 计算各个频道在各个版本下退出文章页对应的url
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.changechannel.ChannelBroadExitUrlMR  -d $yesterday


# 计算各个频道在各个版本下退出文章页总和 以及 涉及url pv
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.changechannel.ChannelExitUrlCountAndUrlPv  -d $yesterday

# 计算各个频道在各个版本下跳出文章页对应的url
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.changechannel.ChannelBounceUrlMR  -d $yesterday

# 计算各个频道在各个版本下跳出文章页总和 以及 涉及url pv
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.changechannel.ChannelBounceUrlCountAndSIDCount  -d $yesterday

# 计算各个频道在各个版本下文章页uuid ViewFocus事件对应的次数
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.changechannel.ChannelBroadViewFocusCountMR  -d $yesterday
# 计算各个频道在各个版本下文章页 channelName+"_"+broad,pgr,uuid  对应的最大pagescrolly
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.changechannel.ChannelBroadMaxPagescrollYCountMR  -d $yesterday

# 计算旅游频道下文章页 ip分布
sh $baseDir/../runMRJob.sh -c com.netease.weblogOffline.statistics.changechannel.IPMR  -d $yesterday

#导数据到DC
${HADOOP} fs  -text ${WEBLOG_TEMP_HDFS_DIR}ChannelofBroadPvUv/$yesterday/p* |awk -F'\t' '{print $1"\t"$2}' |sort -k 1 -nr > ${WEBLOG_RESULT_LOCAL_DIR}list/ChannelofBroadPv$yesterday

${HADOOP} fs  -text ${WEBLOG_TEMP_HDFS_DIR}ChannelofBroadPvUv/$yesterday/p* |awk -F'\t' '{print $1"\t"$3}' |sort -k 1 -nr > ${WEBLOG_RESULT_LOCAL_DIR}list/ChannelofBroadUv$yesterday

${HADOOP} fs -text ${WEBLOG_TEMP_HDFS_DIR}EmailOrNoEmailUvUv/$yesterday/p*  |awk -F'\t' '{print $1"\t"$2}' |sort -k 1 -nr > ${WEBLOG_RESULT_LOCAL_DIR}list/EmailOrNoEmailUvUv$yesterday

${HADOOP} fs -text ${WEBLOG_TEMP_HDFS_DIR}ChannelChannelClickUrlCountAndPv/$yesterday/p* |awk -F'\t'  '{print $1"\t"$2"\t"$3"\t"$2/$3}'|sort -k 1 -nr > ${WEBLOG_RESULT_LOCAL_DIR}list/ClickInfo$yesterday

${HADOOP} fs -text ${WEBLOG_TEMP_HDFS_DIR}ChannelBroadExitUrlCountAndPv/$yesterday/p* |awk -F'\t'  '{print $1"\t"$2"\t"$3"\t" $2/$3*100"%"}' |sort -k 1 -nr >${WEBLOG_RESULT_LOCAL_DIR}list/ExitInfo$yesterday
${HADOOP} fs -text ${WEBLOG_TEMP_HDFS_DIR}ChannelBounceUrlCountAndSIDCount/$yesterday/p* |awk -F'\t'  '{print $1"\t"$2"\t"$3"\t" $2/$3*100"%"}' |sort -k 1 -nr >${WEBLOG_RESULT_LOCAL_DIR}list/BounceInfo$yesterday

#o位奇数，e为偶数
${HADOOP} fs -text ${WEBLOG_TEMP_HDFS_DIR}IP/$yesterday/p*|awk -F'_'  '{print $1"\t"$2}'|awk  '{if($2%2==0) print $1"_e""\t"$3} {if($2%2==1) print $1"_o""\t"$3}' |awk '{a[$1]+=$2;}END{for(i in a){{print i" "a[i];}}}' |sort -k 1 -nr |xargs -n8|awk -F' ' '{print $1"\t"$2"\t"$3"\t"$4"\t"$5"\t"$6"\t"$7"\t"$8"\ta1301_e%\t"$6/($2+$6)"\ta1308_o%\t"$2/($2+$6)}'|awk -F'\t' '{print $1"\t"$2"\n"$3"\t"$4"\n"$5"\t"$6"\n"$7"\t"$8"\na1301_e%\t"$10*100"\na1308_o%\t"$12*100}'>${WEBLOG_RESULT_LOCAL_DIR}list/IPdistributed$yesterday



${HADOOP}  fs -text ${WEBLOG_TEMP_HDFS_DIR}ChannelBroadViewFocusCount/$yesterday/p* |awk -F'_'  '{print $1"\t"$2"\t"$3}'|awk -F'\t'  '{print $1"\t"$2"\t"$4}'|awk '{if($3<11) {print $1"#"$2"_1*10\t"$3 ;}else if($3<21) {print $1"#"$2"_11*20\t"$3 ;}else if($3<31) {print $1"#"$2"_21*30\t"$3 ;}else if($3<41) {print $1"#"$2"_31*40\t"$3 ;}else if($3<51) {print $1"#"$2"_41*50\t"$3 ;}else if($3<61) {print $1"#"$2"_51*60\t"$3 ;}else if($3<71) {print $1"#"$2"_61*70\t"$3 ;}else if($3<81) {print $1"#"$2"_71*80\t"$3 ;}else if($3<91) {print $1"#"$2"_81*90\t"$3 ;}else if($3<=100) {print $1"#"$2"_91*100\t"$3 ;}else if($3>100) {print $1"#"$2"_100*--\t"$3 ;}}'|awk '{a[$1]+=1;}END{for(i in a){{print i"\t"a[i];}}}'|awk -F'_'  '{print $1"\t"$2}'|awk -F'*'  '{print $1"\t"$2}'|grep travel#a1508-waterfall|sort -k 2 -nr |awk -F'\t'  '{print $1"_"$2"*"$3"\t"$4}' >${WEBLOG_RESULT_LOCAL_DIR}list/ViewFocusCount$yesterday
${HADOOP} fs -text ${WEBLOG_TEMP_HDFS_DIR}ChannelBroadViewFocusCount/$yesterday/p* |awk -F'_'  '{print $1"\t"$2"\t"$3}'|awk -F'\t'  '{print $1"\t"$2"\t"$4}'|awk '{if($3<11) {print $1"#"$2"_1*10\t"$3 ;}else if($3<21) {print $1"#"$2"_11*20\t"$3 ;}else if($3<31) {print $1"#"$2"_21*30\t"$3 ;}else if($3<41) {print $1"#"$2"_31*40\t"$3 ;}else if($3<51) {print $1"#"$2"_41*50\t"$3 ;}else if($3<61) {print $1"#"$2"_51*60\t"$3 ;}else if($3<71) {print $1"#"$2"_61*70\t"$3 ;}else if($3<81) {print $1"#"$2"_71*80\t"$3 ;}else if($3<91) {print $1"#"$2"_81*90\t"$3 ;}else if($3<=100) {print $1"#"$2"_91*100\t"$3 ;}else if($3>100) {print $1"#"$2"_100*--\t"$3 ;}}'|awk '{a[$1]+=1;}END{for(i in a){{print i"\t"a[i];}}}'|awk -F'_'  '{print $1"\t"$2}'|awk -F'*'  '{print $1"\t"$2}'|grep travel#a1508-listshow|sort -k 2 -nr |awk -F'\t'  '{print $1"_"$2"*"$3"\t"$4}' >>${WEBLOG_RESULT_LOCAL_DIR}list/ViewFocusCount$yesterday
${HADOOP}  fs -text ${WEBLOG_TEMP_HDFS_DIR}ChannelBroadViewFocusCount/$yesterday/p* |awk -F'_'  '{print $1"\t"$2"\t"$3}'|awk -F'\t'  '{print $1"\t"$2"\t"$4}'|awk '{if($3<11) {print $1"#"$2"_1*10\t"$3 ;}else if($3<21) {print $1"#"$2"_11*20\t"$3 ;}else if($3<31) {print $1"#"$2"_21*30\t"$3 ;}else if($3<41) {print $1"#"$2"_31*40\t"$3 ;}else if($3<51) {print $1"#"$2"_41*50\t"$3 ;}else if($3<61) {print $1"#"$2"_51*60\t"$3 ;}else if($3<71) {print $1"#"$2"_61*70\t"$3 ;}else if($3<81) {print $1"#"$2"_71*80\t"$3 ;}else if($3<91) {print $1"#"$2"_81*90\t"$3 ;}else if($3<=100) {print $1"#"$2"_91*100\t"$3 ;}else if($3>100) {print $1"#"$2"_100*--\t"$3 ;}}'|awk '{a[$1]+=1;}END{for(i in a){{print i"\t"a[i];}}}'|awk -F'_'  '{print $1"\t"$2}'|awk -F'*'  '{print $1"\t"$2}'|grep travel#a1301|sort -k 2 -nr |awk -F'\t'  '{print $1"_"$2"*"$3"\t"$4}' >>${WEBLOG_RESULT_LOCAL_DIR}list/ViewFocusCount$yesterday
awk  -F'#' '{print $1"_"$2}'  ${WEBLOG_RESULT_LOCAL_DIR}list/ViewFocusCount$yesterday|grep travel_a1508 |awk  -F'_' '{print $1"#a1508\t"$3}'|awk -F'\t' '{print $1"_"$2"\t"$3}'|awk '{a[$1]+=$2;}END{for(i in a){{print i"\t"a[i];}}}'| awk -F'_'  '{print $1"\t"$2}'|awk -F'*'  '{print $1"\t"$2}'|grep travel#a1508|sort -k 2 -nr |awk -F'\t'  '{print $1"_"$2"*"$3"\t"$4}' >>${WEBLOG_RESULT_LOCAL_DIR}list/ViewFocusCount$yesterday

${HADOOP}  fs -text ${WEBLOG_TEMP_HDFS_DIR}ChannelBroadMaxPagescrollYCount/$yesterday/p* |awk -F'_'  '{print $1"\t"$2"\t"$4}'|awk -F'\t'  '{print $1"\t"$2"\t"$4}'|awk '{if($3<601) {print $1"#"$2"_0*600\t"$3 ;}else if($3<1201) {print $1"#"$2"_601*1200\t"$3 ;}else if($3<1801) {print $1"#"$2"_1201*1800\t"$3 ;}else if($3<2401) {print $1"#"$2"_1801*2400\t"$3 ;}else if($3<3001) {print $1"#"$2"_2401*3000\t"$3 ;}else if($3<3601) {print $1"#"$2"_3001*3600\t"$3 ;}else if($3<4201) {print $1"#"$2"_3601*4200\t"$3 ;}else if($3<4801) {print $1"#"$2"_4201*4800\t"$3 ;}else if($3<5401) {print $1"#"$2"_4801*5400\t"$3 ;}else if($3<6001) {print $1"#"$2"_5401*6000\t"$3 ;}else if($3<6601) {print $1"#"$2"_6001*6600\t"$3 ;}else if($3<7201) {print $1"#"$2"_6601*7200\t"$3 ;}else if($3<7801) {print $1"#"$2"_7201*7800\t"$3 ;}else if($3<8401) {print $1"#"$2"_7801*8400\t"$3 ;}else if($3<9001) {print $1"#"$2"_8401*9000\t"$3 ;}else if($3<9601) {print $1"#"$2"_9001*9600\t"$3 ;}else if($3<10201) {print $1"#"$2"_9601*10200\t"$3 ;}else if($3<10801) {print $1"#"$2"_10201*10800\t"$3 ;}else if($3<11401) {print $1"#"$2"_10801*11400\t"$3 ;}else if($3<12001) {print $1"#"$2"_11401*12000\t"$3 ;}else if($3>=12001) {print $1"#"$2"_12001*--\t"$3 ;}}'|awk '{a[$1]+=1;}END{for(i in a){{print i"\t"a[i];}}}'|awk -F'_'  '{print $1"\t"$2}'|awk -F'*'  '{print $1"\t"$2}'|grep travel#a1508-waterfall|sort -k 2 -nr |awk -F'\t'  '{print $1"_"$2"*"$3"\t"$4}' >${WEBLOG_RESULT_LOCAL_DIR}list/MaxPagescrollYCount$yesterday
${HADOOP}  fs -text ${WEBLOG_TEMP_HDFS_DIR}ChannelBroadMaxPagescrollYCount/$yesterday/p* |awk -F'_'  '{print $1"\t"$2"\t"$4}'|awk -F'\t'  '{print $1"\t"$2"\t"$4}'|awk '{if($3<601) {print $1"#"$2"_0*600\t"$3 ;}else if($3<1201) {print $1"#"$2"_601*1200\t"$3 ;}else if($3<1801) {print $1"#"$2"_1201*1800\t"$3 ;}else if($3<2401) {print $1"#"$2"_1801*2400\t"$3 ;}else if($3<3001) {print $1"#"$2"_2401*3000\t"$3 ;}else if($3<3601) {print $1"#"$2"_3001*3600\t"$3 ;}else if($3<4201) {print $1"#"$2"_3601*4200\t"$3 ;}else if($3<4801) {print $1"#"$2"_4201*4800\t"$3 ;}else if($3<5401) {print $1"#"$2"_4801*5400\t"$3 ;}else if($3<6001) {print $1"#"$2"_5401*6000\t"$3 ;}else if($3<6601) {print $1"#"$2"_6001*6600\t"$3 ;}else if($3<7201) {print $1"#"$2"_6601*7200\t"$3 ;}else if($3<7801) {print $1"#"$2"_7201*7800\t"$3 ;}else if($3<8401) {print $1"#"$2"_7801*8400\t"$3 ;}else if($3<9001) {print $1"#"$2"_8401*9000\t"$3 ;}else if($3<9601) {print $1"#"$2"_9001*9600\t"$3 ;}else if($3<10201) {print $1"#"$2"_9601*10200\t"$3 ;}else if($3<10801) {print $1"#"$2"_10201*10800\t"$3 ;}else if($3<11401) {print $1"#"$2"_10801*11400\t"$3 ;}else if($3<12001) {print $1"#"$2"_11401*12000\t"$3 ;}else if($3>=12001) {print $1"#"$2"_12001*--\t"$3 ;}}'|awk '{a[$1]+=1;}END{for(i in a){{print i"\t"a[i];}}}'|awk -F'_'  '{print $1"\t"$2}'|awk -F'*'  '{print $1"\t"$2}'|grep travel#a1508-listshow|sort -k 2 -nr |awk -F'\t'  '{print $1"_"$2"*"$3"\t"$4}' >>${WEBLOG_RESULT_LOCAL_DIR}list/MaxPagescrollYCount$yesterday
${HADOOP}  fs -text ${WEBLOG_TEMP_HDFS_DIR}ChannelBroadMaxPagescrollYCount/$yesterday/p* |awk -F'_'  '{print $1"\t"$2"\t"$4}'|awk -F'\t'  '{print $1"\t"$2"\t"$4}'|awk '{if($3<601) {print $1"#"$2"_0*600\t"$3 ;}else if($3<1201) {print $1"#"$2"_601*1200\t"$3 ;}else if($3<1801) {print $1"#"$2"_1201*1800\t"$3 ;}else if($3<2401) {print $1"#"$2"_1801*2400\t"$3 ;}else if($3<3001) {print $1"#"$2"_2401*3000\t"$3 ;}else if($3<3601) {print $1"#"$2"_3001*3600\t"$3 ;}else if($3<4201) {print $1"#"$2"_3601*4200\t"$3 ;}else if($3<4801) {print $1"#"$2"_4201*4800\t"$3 ;}else if($3<5401) {print $1"#"$2"_4801*5400\t"$3 ;}else if($3<6001) {print $1"#"$2"_5401*6000\t"$3 ;}else if($3<6601) {print $1"#"$2"_6001*6600\t"$3 ;}else if($3<7201) {print $1"#"$2"_6601*7200\t"$3 ;}else if($3<7801) {print $1"#"$2"_7201*7800\t"$3 ;}else if($3<8401) {print $1"#"$2"_7801*8400\t"$3 ;}else if($3<9001) {print $1"#"$2"_8401*9000\t"$3 ;}else if($3<9601) {print $1"#"$2"_9001*9600\t"$3 ;}else if($3<10201) {print $1"#"$2"_9601*10200\t"$3 ;}else if($3<10801) {print $1"#"$2"_10201*10800\t"$3 ;}else if($3<11401) {print $1"#"$2"_10801*11400\t"$3 ;}else if($3<12001) {print $1"#"$2"_11401*12000\t"$3 ;}else if($3>=12001) {print $1"#"$2"_12001*--\t"$3 ;}}'|awk '{a[$1]+=1;}END{for(i in a){{print i"\t"a[i];}}}'|awk -F'_'  '{print $1"\t"$2}'|awk -F'*'  '{print $1"\t"$2}'|grep travel#a1301|sort -k 2 -nr |awk -F'\t'  '{print $1"_"$2"*"$3"\t"$4}' >>${WEBLOG_RESULT_LOCAL_DIR}list/MaxPagescrollYCount$yesterday

awk  -F'#' '{print $1"_"$2}'  ${WEBLOG_RESULT_LOCAL_DIR}list/MaxPagescrollYCount$yesterday|grep travel_a1508 |awk  -F'_' '{print $1"#a1508\t"$3}'|awk -F'\t' '{print $1"_"$2"\t"$3}'|awk '{a[$1]+=$2;}END{for(i in a){{print i"\t"a[i];}}}'|awk -F'_'  '{print $1"\t"$2}'|awk -F'*'  '{print $1"\t"$2}'|grep travel#a1508|sort -k 2 -nr |awk -F'\t'  '{print $1"_"$2"*"$3"\t"$4}' >>${WEBLOG_RESULT_LOCAL_DIR}list/MaxPagescrollYCount$yesterday








