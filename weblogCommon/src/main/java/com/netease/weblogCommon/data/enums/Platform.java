package com.netease.weblogCommon.data.enums;

import java.util.HashMap;
import java.util.Map;

public enum Platform {
	
	/** 全平台 */
	all("all", (byte) 0),/** 其它端*/
	other("other", (byte) 1),/** pc端 */
	www("www", (byte) 2),/** wap、3g端 */
	wap("wap", (byte) 3),/** app端 */
	app("app", (byte) 4);/** 跟帖 */
	
	
	private String name;
	private byte compactId;
	
	private static Map<String, Platform> nameIndexMap = new HashMap<>();
	private static Map<Byte, Platform> compactIdIndexMap = new HashMap<>();

	private Platform(String name, byte compactId) {
		this.name = name;
		this.compactId = compactId;
	}
	
	static{
		for(Platform platform : Platform.values()){
			nameIndexMap.put(platform.name, platform);
			compactIdIndexMap.put(platform.compactId, platform);
		}
	}
	
	public String getName(){
		return name;
	}

	public byte getCompactId() {
		return compactId;
	}
	
	public static Byte name2CompactId(String name){
		Platform platform = getByName(name);
		if(null != platform){
			return platform.getCompactId();
		}
		
		return null;
	}
	
	public static String compactId2Name(byte compactId){
		Platform platform = getCompactId(compactId);
		if(null != platform){
			return platform.getName();
		}
		
		return null;
	}
	
	public static Platform getByName(String name){
		return nameIndexMap.get(name);
	}
	
	public static Platform getCompactId(byte compactId){
		return compactIdIndexMap.get(compactId);
	}
}
