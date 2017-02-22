package com.netease.weblogCommon.logparsers;

import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

import com.netease.weblogCommon.data.enums.SimpleDateFormatEnum;
import com.netease.weblogCommon.utils.DateUtils;
import com.netease.weblogCommon.utils.TextUtils;

public class ZylogParser implements LogParser {
	public static final String defNullStr = "(null)";
	
	@Override
	public Map<String, String> parse(String line) {
		Map<String, String> result = new HashMap<String, String>();
		try {
			String unescapeLine = TextUtils.quickUnescape(line);
			String unescapeLineFormat = unescapeLine;
			while(unescapeLineFormat.contains("\t\t")){
				unescapeLineFormat = unescapeLineFormat.replace("\t\t", "\t"+ defNullStr+"\t");
			}
			String[] strs = unescapeLineFormat.split("\t");
			
			long time = DateUtils.toLongTime(strs[0], SimpleDateFormatEnum.zyLogTimeFormat.get());
			
			result.put(ZyLogParams.logTime, String.valueOf(time));
			result.put(ZyLogParams.logTimeFormat, DateUtils.getTime(time, SimpleDateFormatEnum.logTimeFormat.get()));
			
			result.put(ZyLogParams.uid, strs[1]);
			result.put(ZyLogParams.nvtm, strs[2]);
			result.put(ZyLogParams.nvfi, strs[3]);
			result.put(ZyLogParams.nvsf, strs[4]);
			result.put(ZyLogParams.loginStatus, strs[5]);
			result.put(ZyLogParams.url, TextUtils.quickUnescape(strs[6]));
			result.put(ZyLogParams.title, strs[7].contains("%") ? TextUtils.quickUnescape(strs[7]) : strs[7]);
			result.put(ZyLogParams.reference, TextUtils.quickUnescape(strs[8]));
			result.put(ZyLogParams.userAgent, strs[9]);
			result.put(ZyLogParams.resolution, strs[10]);
			result.put(ZyLogParams.langurage, strs[11]);
			result.put(ZyLogParams.screenColorDepth, strs[12]);
			result.put(ZyLogParams.lastModifyTime, strs[13]);
			result.put(ZyLogParams.ip, strs[14]);
			result.put(ZyLogParams.province, strs[15]);
			result.put(ZyLogParams.city, strs[16]);
			result.put(ZyLogParams.supporter, strs[17]);
			result.put(ZyLogParams.email, strs[18]);
			result.put(ZyLogParams.flashVersion, strs[19]);
		} catch (Exception e) {}
			
		return result;
	}
}
