package com.netease.weblogCommon.logparsers;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.netease.weblogCommon.data.enums.SimpleDateFormatEnum;
import com.netease.weblogCommon.utils.DateUtils;
import com.netease.weblogCommon.utils.JsonUtils;
import com.netease.weblogCommon.utils.TextUtils;

public class WeblogParser implements LogParser {

	private boolean debug = false;
	public static final String defStr = "";
	
	private SimpleDateFormat weblogServerTimeFormat = SimpleDateFormatEnum.weblogServerTimeFormat.get();
	private SimpleDateFormat logTimeFormat = SimpleDateFormatEnum.logTimeFormat.get();

	@Override
	public Map<String, String> parse(String line) {
		Map<String, String> result = new HashMap<String, String>();

		String[] items = line.split(" ");

		String udfParamsPart = null;
		StringBuilder browserInfoSB = new StringBuilder();
		boolean browserInfoBegin = false;

		result.put("ip", items[0]);

		// 解析服务器时间
		String serverTimeOriginStr = items[1].trim();
		if (serverTimeOriginStr.startsWith("[")) {
			serverTimeOriginStr = serverTimeOriginStr.substring(1);
		}
		long serverTime = 0;
		String serverTimeFormat = null;
		try {
			serverTime = weblogServerTimeFormat.parse(serverTimeOriginStr).getTime();
			serverTimeFormat = DateUtils.getTime(serverTime, logTimeFormat);
		} catch (ParseException e) {
			serverTime = 0;
			serverTimeFormat = "";
		}
		result.put("serverTime", 0 == serverTime ? defStr : String.valueOf(serverTime));
		result.put("serverTimeFormat", serverTimeFormat);

		for (String item : items) {
			if (item.startsWith("/stat/?")) {
				udfParamsPart = item.replaceAll("/stat/\\?", "");
			}

			if (item.startsWith("HTTP")) {
				browserInfoBegin = true;
			}
			
			if (browserInfoBegin && !item.startsWith("\"http:") && !item.startsWith("http:")) {
				browserInfoSB.append(item).append(" ");
			}
		}

		result.put("browserInfo", stringProcess(browserInfoSB.toString().trim()));

		if (null != udfParamsPart) {
			String[] udfParams = udfParamsPart.split("&");
			for (String s : udfParams) {
				try {
					int first = s.indexOf("=");
					if (first != -1) {
						result.put(s.substring(0, first), stringProcess(s.substring(first + 1, s.length())));
					}
				} catch (Exception e) {
					if(debug){
						e.printStackTrace();
					}
				}
			}
		}
		
		try {
			String cvar_origin = stringProcess(result.get("cvar"));
			String cvar = new String(cvar_origin.getBytes("ISO-8859-1"), "UTF-8");

			if (null != cvar && cvar.length() > 2) {
				Map<String, String> cvarMap = JsonUtils.json2Map(cvar);
				if (null != cvarMap) {
					for (Entry<String, String> e : cvarMap.entrySet()) {
						result.put("cvar_" + e.getKey(), stringProcess(e.getValue()));
					}
				}
			}
		} catch (Exception e) {
			if(debug){
				e.printStackTrace();
			}
		}
		
		try {
			String cdata_origin = stringProcess(result.get("cdata"));
			String cdata = new String(cdata_origin.getBytes("ISO-8859-1"), "UTF-8");

			if (null != cdata && cdata.length() > 2) {
				Map<String, String> cdataMap = JsonUtils.json2Map(cdata);
				if (null != cdataMap) {
					for (Entry<String, String> e : cdataMap.entrySet()) {
						result.put("cdata_" + e.getKey(), stringProcess(e.getValue()));
					}
				}
			}

		} catch (Exception e) {
			if(debug){
				e.printStackTrace();
			}
		}
		
		return result;
	}
	private String stringProcess(String s) {
		return TextUtils.notNullStr(TextUtils.quickUnescape(s), defStr);
	}
	
	public void setDebug(boolean debug) {
		this.debug = debug;
	}
}


