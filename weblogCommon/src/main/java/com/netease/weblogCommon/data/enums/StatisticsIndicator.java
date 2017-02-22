package com.netease.weblogCommon.data.enums;

import java.util.HashMap;
import java.util.Map;

public enum StatisticsIndicator {
	
	/** 基础pv、uv */
	base("base", (byte) 1),/** 会话 */ 
	sessionCount("sessionCount", (byte) 2),/** 分享 */
	share("share", (byte) 3),/** 回流 */
	back("back", (byte) 4),/** 跟帖 */
	genTie("genTie", (byte) 5),/** 跟帖顶 */
	genTieUp("genTieUp", (byte) 6),/** 跟帖踩 */
	genTieDown("genTieDown", (byte) 7),/** 原内容总顶 */
	contentUp("contentUp", (byte) 8),/** 原内容总踩 */
	contentDown("contentDown", (byte) 9);
	
	private String name;
	private byte compactId;
	
	private static Map<String, StatisticsIndicator> nameIndexMap = new HashMap<>();
	private static Map<Byte, StatisticsIndicator> compactIdIndexMap = new HashMap<>();

	private StatisticsIndicator(String name, byte compactId) {
		this.name = name;
		this.compactId = compactId;
	}
	
	static{
		for(StatisticsIndicator si : StatisticsIndicator.values()){
			nameIndexMap.put(si.name, si);
			compactIdIndexMap.put(si.compactId, si);
		}
	}
	
	public String getName(){
		return name;
	}

	public byte getCompactId() {
		return compactId;
	}
	
	public static Byte name2CompactId(String name){
		StatisticsIndicator si = getByName(name);
		if(null != si){
			return si.getCompactId();
		}
		
		return null;
	}
	
	public static String compactId2Name(byte compactId){
		StatisticsIndicator si = getCompactId(compactId);
		if(null != si){
			return si.getName();
		}
		
		return null;
	}
	
	public static StatisticsIndicator getByName(String name){
		return nameIndexMap.get(name);
	}
	
	public static StatisticsIndicator getCompactId(byte compactId){
		return compactIdIndexMap.get(compactId);
	}
}
