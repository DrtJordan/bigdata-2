package com.netease.weblogOffline.common.zylogfilter;

import java.net.URLDecoder;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;

import com.netease.weblogCommon.data.enums.SimpleDateFormatEnum;
import com.netease.weblogCommon.logparsers.ZyLogParams;
import com.netease.weblogCommon.utils.DateUtils;
import com.netease.weblogCommon.utils.TextUtils;


public class ZylogFilterUtils {
	
	public static final String defNullStr = "(null)";
	private static final String[] columns = {"logTime","uid","nvtm","nvfi","nvsf","loginStatus","url","title","reference",
		"userAgent","resolution","langurage","screenColorDepth","lastModifyTime","ip","province","city","supporter","email","flashVersion","logTimeFormat"};
	
	//为了效率，这里没限制数组内容不能修改，为了程序的正确行，请不要修改获取的数组
	public static String[] getColumns(){
		return columns;
	}
	
	public static HashMap<String, String> buildKVMap(String line){
		if(StringUtils.isBlank(line)){
			return null;
		}
		
		HashMap<String, String> res = new HashMap<String, String>();
		
		String[] items = line.toString().split("\t");
		String[] columnsLocal = getColumns();
		if(items.length == columnsLocal.length){
			for(int i = 0; i < columnsLocal.length; i++){
				res.put(columnsLocal[i], items[i]);
			}
		}
		
		return res;
	}
	
	public static HashMap<String, String> zylogParser(String line){
		HashMap<String, String> result = new HashMap<String, String>();
		try {
			String unescapeLine = TextUtils.quickUnescape(line);
			String unescapeLineFormat = unescapeLine;
			while(unescapeLineFormat.contains("\t\t")){
				unescapeLineFormat = unescapeLineFormat.replace("\t\t", "\t"+ZylogFilterUtils.defNullStr+"\t");
			}
			String[] strs = unescapeLineFormat.split("\t");
			
			long time = DateUtils.toLongTime(TextUtils.notNullStr(strs[0],defNullStr), SimpleDateFormatEnum.zyLogTimeFormat.get());
			
			result.put(ZyLogParams.logTime, String.valueOf(time));
			result.put(ZyLogParams.logTimeFormat, DateUtils.getTime(time, SimpleDateFormatEnum.logTimeFormat.get()));
			
			result.put(ZyLogParams.uid, TextUtils.notNullStr(strs[1],defNullStr));
			result.put(ZyLogParams.nvtm, TextUtils.notNullStr(strs[2],defNullStr));
			result.put(ZyLogParams.nvfi, TextUtils.notNullStr(strs[3],defNullStr));
			result.put(ZyLogParams.nvsf, TextUtils.notNullStr(strs[4],defNullStr));
			result.put(ZyLogParams.loginStatus, TextUtils.notNullStr(strs[5],defNullStr));
			result.put(ZyLogParams.url, TextUtils.quickUnescape(strs[6]));
			result.put(ZyLogParams.title, strs[7].contains("%") ? TextUtils.quickUnescape(strs[7]) : TextUtils.notNullStr(strs[7],defNullStr));
			result.put(ZyLogParams.reference, TextUtils.quickUnescape(strs[8]));
			result.put(ZyLogParams.userAgent, TextUtils.notNullStr(strs[9],defNullStr));
			result.put(ZyLogParams.resolution, TextUtils.notNullStr(strs[10],defNullStr));
			result.put(ZyLogParams.langurage, TextUtils.notNullStr(strs[11],defNullStr));
			result.put(ZyLogParams.screenColorDepth, TextUtils.notNullStr(strs[12],defNullStr));
			result.put(ZyLogParams.lastModifyTime, TextUtils.notNullStr(strs[13],defNullStr));
			result.put(ZyLogParams.ip, TextUtils.notNullStr(strs[14],defNullStr));
			result.put(ZyLogParams.province, TextUtils.notNullStr(new String(strs[15].getBytes("ISO-8859-1"),"UTF-8"),defNullStr));
			result.put(ZyLogParams.city, TextUtils.notNullStr(new String(strs[16].getBytes("ISO-8859-1"),"UTF-8"),defNullStr));
			result.put(ZyLogParams.supporter, TextUtils.notNullStr(new String(strs[17].getBytes("ISO-8859-1"),"UTF-8"),defNullStr));
			result.put(ZyLogParams.email, TextUtils.notNullStr(strs[18],defNullStr));
			result.put(ZyLogParams.flashVersion, TextUtils.notNullStr(strs[19],defNullStr));
		} catch (Exception e) {}
			
		return result;
	}
	
}
