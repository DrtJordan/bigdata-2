use weblog;add jar /home/weblog/webAnalysis/webAnalysis/weblogOffline-0.1.0-jar-with-dependencies.jar;set mapred.job.queue.name=q_weblog;

create external table weblog 
PARTITIONED BY(dt STRING)
row format serde 'com.netease.weblogOffline.hive.WeblogSerde'
stored as textfile;

use weblog;add jar /home/weblog/webAnalysis/webAnalysis/weblogOffline-0.1.0-jar-with-dependencies.jar;set mapred.job.queue.name=q_weblog;
create external table weblog_test 
row format serde 'com.netease.weblogOffline.hive.WeblogSerde'
stored as textfile
location '/ntes_weblog/163com/test/';


use weblog;add jar /home/weblog/webAnalysis/webAnalysis/weblogOffline-0.1.0-jar-with-dependencies.jar;set mapred.job.queue.name=q_weblog;
create external table zylog
PARTITIONED BY(dt STRING)
row format serde 'com.netease.weblogOffline.hive.ZyLogSerde'
stored as textfile;

use weblog;add jar /home/weblog/webAnalysis/webAnalysis/weblogOffline-0.1.0-jar-with-dependencies.jar;set mapred.job.queue.name=q_weblog;
create external table zylog_test
row format serde 'com.netease.weblogOffline.hive.ZyLogSerde'
stored as textfile
location '/ntes_weblog/163com_zy/test/';

use weblog;add jar /home/weblog/webAnalysis/webAnalysis/weblogOffline-0.1.0-jar-with-dependencies.jar;set mapred.job.queue.name=q_weblog;
create external table gentieFilter(url                      string                  ,   
pdocid              string                  ,   
docid              string                  ,  
channel                 string                  ,   
pv                   string                  ,   
uv                string             )
PARTITIONED BY(dt STRING) row format delimited  fields terminated by ',' stored as textfile;
stored as textfile;

use weblog;add jar /home/weblog/webAnalysis/webAnalysis/weblogOffline-0.1.0-jar-with-dependencies.jar;set mapred.job.queue.name=q_weblog;
create external table weblogfilter(ip                      string                  ,   
servertime              string                  ,   
project                 string                  ,   
event                   string                  ,   
esource                 string                  ,   
einfo                   string                  ,   
cost                    string                  ,   
uuid                    string                  ,   
url                     string                  ,   
pureurl                 string                  ,   
contentchannel          string                  ,   
urldomain               string                  ,   
ref                     string                  ,   
refdomain               string                  ,   
pver                    string                  ,   
cdata                   string                  ,   
pagescroll              string                  ,   
pagescrollx             string                  ,   
pagescrolly             string                  ,   
sid                     string                  ,   
ptype                   string                  ,   
entry                   string                  ,   
pgr                     string                  ,   
prev_pgr                string                  ,   
avlbsize                string                  ,   
avlbsizex               string                  ,   
avlbsizey               string                  ,   
resolution              string                  ,   
r                       string                  ,   
ccount                  string                  ,   
ctotalcount             string                  ,   
csource                 string                  ,   
unew                    string                  ,   
utime                   string                  ,   
uctime                  string                  ,   
ultime                  string                  ,   
email                   string                  ,   
cdata_pagex             string                  ,   
cdata_pagey             string                  ,   
cdata_time              string                  ,   
cdata_button            string                  ,   
cdata_tag               string                  ,   
cdata_href              string                  ,   
cdata_text              string                  ,   
cdata_img               string                  ,   
cdata_jcid              string                  ,   
cdata_page              string                  ,   
browserinfo             string                  ,   
extra_pagey             string                  ,   
extra_pagex             string                    ) PARTITIONED BY(dt STRING) row format delimited  fields terminated by '\t' stored as textfile;

use weblog;add jar /home/weblog/webAnalysis/webAnalysis/weblogOffline-0.1.0-jar-with-dependencies.jar;set mapred.job.queue.name=q_weblog;
create external table weblogfilter
PARTITIONED BY(dt STRING)
row format serde 'com.netease.weblogOffline.hive.WeblogfilterSerde'
stored as textfile;

create  database if not exists bigdatahouse;

use bigdatahouse;add jar /home/weblog/webAnalysis/webAnalysis/weblogOffline-0.1.0-jar-with-dependencies.jar;set mapred.job.queue.name=weblog;
create external table YcInfo 
PARTITIONED BY(dt STRING)
row format serde 'com.netease.weblogOffline.statistics.bigdatahouse.YcSerde' 
stored as sequencefile;

use bigdatahouse;add jar /home/weblog/webAnalysis/webAnalysis/weblogOffline-0.1.0-jar-with-dependencies.jar;set mapred.job.queue.name=weblog;
create external table ArticleInfo 
PARTITIONED BY(dt STRING)
row format serde 'com.netease.weblogOffline.statistics.bigdatahouse.MediaSerde' 
stored as sequencefile;

use bigdatahouse;add jar /home/weblog/webAnalysis/webAnalysis/weblogOffline-0.1.0-jar-with-dependencies.jar;set mapred.job.queue.name=weblog;
create external table GenTieLog 
PARTITIONED BY(dt STRING)
row format serde 'com.netease.weblogOffline.statistics.bigdatahouse.GenTieSerde' 
stored as textfile;

use bigdatahouse;add jar /home/weblog/webAnalysis/webAnalysis/weblogOffline-0.1.0-jar-with-dependencies.jar;set mapred.job.queue.name=weblog;
create external table OriginUrlStatistics 
PARTITIONED BY(dt STRING)
row format serde 'com.netease.weblogOffline.statistics.bigdatahouse.DailyUrlInfoSerde' 
stored as sequencefile;

use bigdatahouse;add jar /home/weblog/webAnalysis/webAnalysis/weblogOffline-0.1.0-jar-with-dependencies.jar;set mapred.job.queue.name=weblog;
create external table PureUrlStatistics 
PARTITIONED BY(dt STRING)
row format serde 'com.netease.weblogOffline.statistics.bigdatahouse.DailyPureUrlInfoSerde' 
stored as sequencefile;

create external table zylogfilter PARTITIONED BY(dt STRING)
row format serde 'com.netease.weblogOffline.statistics.bigdatahouse.ZylogfilterSerde' 
stored as sequencefile;

create external table weblogfilter PARTITIONED BY(dt STRING)
row format serde 'com.netease.weblogOffline.statistics.bigdatahouse.WeblogfilterSerde'
stored as sequencefile;


