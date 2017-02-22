package com.netease.weblogOffline.statistics.bigdatahouse;


import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import com.netease.weblogCommon.data.enums.NeteaseChannel_CS;

@Description(
        name = "getUrlChannel",
        value = "NeteaseChannel.getChannelName(url)\n"
        )
public class GetUrlChannelUDF extends UDF {
    public String evaluate(String url){
		String channel = NeteaseChannel_CS.getChannelName(url);
 
        return channel;
    }
}
