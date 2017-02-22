package com.netease.weblogCommon.data.enums;

import java.text.SimpleDateFormat;
import java.util.Locale;

public enum SimpleDateFormatEnum {
 	timeFormat("yyyyMMdd HH:mm:ss"),
	dateFormat("yyyyMMdd"),
	monthFormat("yyyyMM"),
	logTimeFormat("yyyyMMddHHmmss"),
	weblogServerTimeFormat("dd/MMM/yyyy:HH:mm:ss",Locale.US),
 	zyLogTimeFormat("yyyy-MM-dd HH:mm:ss"),
    solrTimeFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
 	
	
	String formatStr = null;
	Locale locale = null;
	private SimpleDateFormatEnum(String formatStr) {
		this.formatStr = formatStr;
	}
	private SimpleDateFormatEnum(String formatStr, Locale locale) {
		this.formatStr = formatStr;
		this.locale = locale;
	}
	
	public SimpleDateFormat get(){
		return null == locale ? new SimpleDateFormat(formatStr) : new SimpleDateFormat(formatStr, locale);
	}
}
