package com.netease.weblogCommon.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

public class DirUtils {
	
	public static String getMobilePartitionPath(String prefix, String date){
		String res = null;
		if(StringUtils.isNotBlank(prefix) && StringUtils.isNotBlank(date)){
			res = prefix;
			if(!prefix.endsWith("/")){
				res += "/";
			}
			String month = date.substring(0, 6);
			res += "month=" + month + "/day=" + date; 
		}
		
		return res;
	}
	
	public static final String[] hours = new String[]{"00","01","02","03","04","05","06","07","08","09","10","11","12","13","14","15","16","17","18","19","20","21","22","23"};
	
	public static List<String> getMobileHourPath(String prefix, String date){
		List<String> res = new ArrayList<String>();
		if(StringUtils.isNotBlank(prefix) && StringUtils.isNotBlank(date)){
			if(!prefix.endsWith("/")){
				prefix += "/";
			}
			String month = date.substring(0,6);
			for(String hour : hours){
				res.add(prefix+"month="+month+"/day="+date+"/hour="+date+hour);
			}
		}
		return res;
	}
}
