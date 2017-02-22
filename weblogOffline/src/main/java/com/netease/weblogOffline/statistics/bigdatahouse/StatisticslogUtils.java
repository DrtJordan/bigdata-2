package com.netease.weblogOffline.statistics.bigdatahouse;

import java.util.HashMap;
//以weblog命名为基准， 统一章鱼命名

public class StatisticslogUtils {

	public static final String defNullStr = "(null)";
	private static final String[] zycolumns = { "logTime", "uid", "url",
			"reference", "userAgent", "ip", "email" };

	private static final String[] weblogcolumns = { "serverTime", "uuid",
			"url", "ref", "browserInfo", "ip", "cvar_email" };

	// 为了效率，这里没限制数组内容不能修改，为了程序的正确行，请不要修改获取的数组
	public static String[] getZycolumns() {
		return zycolumns;
	}

	public static String[] getWeblogcolumns() {
		return weblogcolumns;
	}

	public static HashMap<String, String> weblogBuildKVMap(
			HashMap<String, String> hm) {
		if (hm == null) {
			return null;
		}

		HashMap<String, String> res = new HashMap<String, String>();

		String[] weblogcolumns= getWeblogcolumns();
	
		for (String s : weblogcolumns) {
			res.put(s , hm.get(s));
		}
		return res;
	}


	public static HashMap<String, String> zyBuildKVMap(
			HashMap<String, String> hm) {
		if (hm == null) {
			return null;
		}

		HashMap<String, String> res = new HashMap<String, String>();

		String[] zycolumns= getZycolumns();
		String[] weblogcolumns= getWeblogcolumns();
		
		for (int i = 0 ;i<weblogcolumns.length;i++) {
				res.put(weblogcolumns[i],hm.get(zycolumns[i]));
		}
		return res;
	}
}
