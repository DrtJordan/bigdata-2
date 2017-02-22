package com.netease.weblogCommon.tools;

import com.netease.weblogCommon.data.enums.Platform;
import com.netease.weblogCommon.data.enums.ShareBackChannel_EE;
import com.netease.weblogCommon.data.enums.StatisticsIndicator;


public class EditorEvaluationKeyBuilder {
	
	private static final String regex = "_";
	
	public static String getColumnName(StatisticsIndicator si, Platform pf, ShareBackChannel_EE sbc){
		return getColumnName(si, pf, sbc.getName());
	}
	
	public static String getColumnName(StatisticsIndicator si, Platform pf, String extraStr){
		return si.getName() + regex + pf.getName() + regex + extraStr;
	}

	
	/**
	 * @param key
	 * @param isUv true:uv;false:pv
	 * @return
	 */
    public static Integer compactColumnName(String key) {
        String[] strs = key.split(regex);
        try {
            byte statisticsIndicator = StatisticsIndicator.name2CompactId(strs[0]);
            byte platform = Platform.name2CompactId(strs[1]);
            byte shareBackChannel = ShareBackChannel_EE.name2CompactId(strs[2]);

            return (statisticsIndicator << 24) + (platform << 16) + (shareBackChannel << 8);
        } catch (Exception e) {
            return null;
        }

    }
	
    public static String uncompactColumnName(int i) {
        byte statisticsIndicator = (byte) (i >> 24 & 0xFF);
        byte platform = (byte) (i >> 16 & 0xFF);
        byte shareBackChannel = (byte) (i >> 8 & 0xFF);
        try {
            return getColumnName(StatisticsIndicator.getCompactId(statisticsIndicator),
                    Platform.getCompactId(platform), ShareBackChannel_EE.getCompactId(shareBackChannel).getName());
        } catch (Exception e) {
            return null;
        }
    }
	
	public static void main(String[] args) {
		long b1 = System.currentTimeMillis();
		StatisticsIndicator si = StatisticsIndicator.back;
		Platform p = Platform.all;
		ShareBackChannel_EE s= ShareBackChannel_EE.all;
		long b2 = System.currentTimeMillis();
		String columnName = getColumnName(StatisticsIndicator.base,Platform.app,ShareBackChannel_EE.weiBo.getName());
		System.out.println(columnName);
		int i = compactColumnName(columnName);
		System.out.println(i);
		System.out.println(uncompactColumnName(i));
		long e = System.currentTimeMillis();
		System.out.println(b2 - b1);
		System.out.println(e - b2);
	}
}
