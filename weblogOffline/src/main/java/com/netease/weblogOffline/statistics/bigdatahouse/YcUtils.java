package com.netease.weblogOffline.statistics.bigdatahouse;

import java.util.HashMap;

import org.apache.hadoop.io.Text;

import com.netease.weblogCommon.data.enums.NeteaseContentType;
import com.netease.weblogCommon.utils.TextUtils;
import com.netease.weblogOffline.data.HashMapStringStringWritable;


public class YcUtils {
	
	
	public static void main(String[] args){

	}
	public static final String defNullStr = "(null)";
	private static final String[] columns = {"lmodify","url","title","ptime","type","date","author"};
	
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
  		//lmodify  url  title  ptime  or and author
		String[] strs = line.split("\t");
		if(strs.length >= 4){
			outputValue.getHm().put("lmodify", TextUtils.notNullStr(strs[0],defNullStr));
			outputValue.getHm().put("url", TextUtils.notNullStr(strs[1],defNullStr));
			outputValue.getHm().put("title", TextUtils.notNullStr(strs[2],defNullStr));
   			outputValue.getHm().put("ptime", TextUtils.notNullStr(strs[3],defNullStr));
   			outputValue.getHm().put("type", NeteaseContentType.getTypeName(strs[1]));
   		  	if(strs.length >= 5){
 		  	   outputValue.getHm().put("author", TextUtils.notNullStr(strs[4],defNullStr));
 		  	}
   			return outputValue;
		}else {
			return null;
		}

	}
	
}
