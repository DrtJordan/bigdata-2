package com.netease.weblogCommon.data.enums;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

public enum ShareBackChannel_EE {
	
	/** 全平台 */
	all("all", (byte) 0, "", ""), /** 其它 */
	other("other", (byte) 1, "", ""),/** 微信 */
	weiXin("weiXin", (byte) 2, ShareBackChannel_CS.weixin.getName(), "wx"),/** 微博 */
	weiBo("weiBo", (byte) 3, ShareBackChannel_CS.weibo.getName(), "wb"),/** 易信 */
	yiXin("yiXin", (byte) 4, ShareBackChannel_CS.yixin.getName(), "yx"),/** qq*/
	qq("qq", (byte) 5, ShareBackChannel_CS.qzone.getName(), "qq");

	private String name;
	private byte compactId;
	private String sign_3w;
	private String sign_sps;

	
	private static Map<String, ShareBackChannel_EE> nameIndexMap = new HashMap<>();
	private static Map<String, ShareBackChannel_EE> sign_3wIndexMap = new HashMap<>();
	private static Map<String, ShareBackChannel_EE> sign_spsIndexMap = new HashMap<>();
	private static Map<Byte, ShareBackChannel_EE> compactIdIndexMap = new HashMap<>();

	private ShareBackChannel_EE(String name, byte compactId, String sign_3w, String sign_sps) {
		this.name = name;
		this.compactId = compactId;
		this.sign_3w = sign_3w;
		this.sign_sps = sign_sps;
	}
	
	static{
		for(ShareBackChannel_EE shareBackChannel : ShareBackChannel_EE.values()){
			nameIndexMap.put(shareBackChannel.name, shareBackChannel);
			compactIdIndexMap.put(shareBackChannel.compactId, shareBackChannel);
			if(StringUtils.isNotBlank(shareBackChannel.sign_3w)){
				sign_3wIndexMap.put(shareBackChannel.sign_3w, shareBackChannel);
			}
			if(StringUtils.isNotBlank(shareBackChannel.sign_sps)){
				sign_3wIndexMap.put(shareBackChannel.sign_sps, shareBackChannel);
			}
		}
	}
	
	public String getName(){
		return name;
	}

	public byte getCompactId() {
		return compactId;
	}
	
	public static Byte name2CompactId(String name){
		ShareBackChannel_EE shareBackChannel = getByName(name);
		if(null != shareBackChannel){
			return shareBackChannel.getCompactId();
		}
		
		return null;
	}
	
	public static String compactId2Name(byte compactId){
		ShareBackChannel_EE shareBackChannel = getCompactId(compactId);
		if(null != shareBackChannel){
			return shareBackChannel.getName();
		}
		
		return null;
	}
	
	public static ShareBackChannel_EE getByName(String name){
		return nameIndexMap.get(name);
	}
	
	public static ShareBackChannel_EE getCompactId(byte compactId){
		return compactIdIndexMap.get(compactId);
	}
	
	public static ShareBackChannel_EE getBySign_3w(String sign_3w){
		ShareBackChannel_EE res = sign_3wIndexMap.get(sign_3w);
		if(null == res){
			res = ShareBackChannel_EE.other;
		}
		return res;
	}
	
	public static ShareBackChannel_EE getBySign_sps(String sign_sps){
		ShareBackChannel_EE res = sign_spsIndexMap.get(sign_sps);
		if(null == res){
			res = ShareBackChannel_EE.other;
		}
		return res;
	}

}
