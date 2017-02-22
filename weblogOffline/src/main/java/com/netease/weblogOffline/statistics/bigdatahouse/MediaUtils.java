package com.netease.weblogOffline.statistics.bigdatahouse;

import java.net.URL;
import java.util.HashMap;

import org.apache.hadoop.io.Text;

import com.netease.weblogCommon.utils.TextUtils;
import com.netease.weblogOffline.data.HashMapStringStringWritable;


public class MediaUtils {
	
	
	public static void main(String[] args){

	}
	public static final String defNullStr = "(null)";
	private static final String[] columns = {"url",
	"title",
	"dkeys",
	"topicname",
	"mediasource",
	"date"
};
	
	//为了效率，这里没限制数组内容不能修改，为了程序的正确行，请不要修改获取的数组
	public static String[] getColumns(){
		return columns;
	}
	
	public static HashMap<String, String> buildKVMap(HashMapStringStringWritable lineMap){

		
		HashMap<String, String> res = new HashMap<String, String>();
		
		String[] columnsLocal = getColumns();

			for(String column:columnsLocal){
				res.put(column, lineMap.getHm().get(column)==null?defNullStr:TextUtils.notNullStr(lineMap.getHm().get(column),defNullStr));
			}
		
		
		return res;
	}
	
	public static HashMapStringStringWritable logParser(Text value){
 		String line = value.toString();
    	HashMapStringStringWritable outputValue = new HashMapStringStringWritable();
		//url title dkeys topicname source
		String[] strs = line.split("\t");
		try {
			URL url = new URL(TextUtils.notNullStr(strs[0],defNullStr));
		}catch (Exception e) {
		   return null;
		}

	
		if(strs.length == 5){
			outputValue.getHm().put("url", TextUtils.notNullStr(strs[0],defNullStr));
			outputValue.getHm().put("title", TextUtils.notNullStr(strs[1],defNullStr));
			outputValue.getHm().put("dkeys", TextUtils.notNullStr(strs[2],defNullStr));
   			outputValue.getHm().put("topicname", TextUtils.notNullStr(strs[3],defNullStr));
   			outputValue.getHm().put("mediasource", TextUtils.notNullStr(strs[4].split("&&&")[0],defNullStr));
   			return outputValue;
		}else if (strs.length > 5){
			outputValue.getHm().put("url", TextUtils.notNullStr(strs[0],defNullStr));
			outputValue.getHm().put("title", MediaUtils.defNullStr);
			outputValue.getHm().put("dkeys", MediaUtils.defNullStr);
   			outputValue.getHm().put("topicname", MediaUtils.defNullStr);
   			outputValue.getHm().put("mediasource", TextUtils.notNullStr(strs[strs.length-1].split("&&&")[0],defNullStr));
   			return outputValue;
		}else {
			return null;
		}
	}
	
}
