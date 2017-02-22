package com.netease.weblogCommon.utils;

import org.apache.commons.lang3.StringUtils;

public class DocidUtils {
	
	public static String getChannelId(String docid){
		if(StringUtils.isBlank(docid) || docid.length()!=16){
			return null;
		}
		
		return docid.substring(8,12);
	}
	
	public static String getTopicId(String docid){
		if(StringUtils.isBlank(docid) || docid.length()!=16){
			return null;
		}
		
		return docid.substring(8,16);
	}
	
	public static void main(String[] args) {
		String docid = "BDOEVCQF00014AED";
		
		System.out.println(getChannelId(docid));
		System.out.println(getTopicId(docid));
	}

}
