package com.netease.weblogOffline.statistics.bigdatahouse;

import java.util.HashMap;

import org.apache.hadoop.io.Text;

import com.netease.weblogCommon.utils.TextUtils;



public class GenTieUtils {
	
	
	public static void main(String[] args){

	}
	public static final String defNullStr = "(null)";
	private static final String[] columns = {"url",
	 "pdocid",
	 "docid",
	 "uuid",
	 "gentietime",
	 "gentieid",
	 "ip",
	 "gentiesource"};
	//这里的source是指端，如ph
	//为了效率，这里没限制数组内容不能修改，为了程序的正确行，请不要修改获取的数组
	public static String[] getColumns(){
		return columns;
	}
	
	
	public static HashMap<String,String> logParser(Text value){
 		String line = value.toString();
 		HashMap<String,String> outputValue = new HashMap<String,String>();
    	// url,pdocid,docid,发帖用户id,跟帖时间，跟帖id,ip,source
		String[] strs = line.split(",");
		if(strs.length>=8){
			outputValue.put("url", TextUtils.notNullStr(strs[0],defNullStr));
			outputValue.put("pdocid", TextUtils.notNullStr(strs[1],defNullStr));
			outputValue.put("docid", TextUtils.notNullStr(strs[2],defNullStr));
   			outputValue.put("uuid", TextUtils.notNullStr(strs[3],defNullStr));
   			outputValue.put("gentietime", TextUtils.notNullStr(strs[4],defNullStr));
			outputValue.put("gentieid", TextUtils.notNullStr(strs[5],defNullStr));
   			outputValue.put("ip", TextUtils.notNullStr(strs[6],defNullStr));
   			outputValue.put("gentiesource", TextUtils.notNullStr(strs[7],defNullStr));
   			return outputValue;
		}else {
			return null;
		}
		

	}
	
}
