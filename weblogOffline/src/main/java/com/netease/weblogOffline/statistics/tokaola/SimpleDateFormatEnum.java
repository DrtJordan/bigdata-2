package com.netease.weblogOffline.statistics.tokaola;

import java.text.SimpleDateFormat;
import java.util.Locale;

public enum SimpleDateFormatEnum {
 	timeFormat("yyyyMMdd HH:mm:ss"),
	dateFormat("yyyyMMdd"),
	monthFormat("yyyyMM"),
	logTimeFormat("yyyyMMddHHmmss"),
    ngnixServerTimeFormat("dd/MMM/yyyy:HH:mm:ss",Locale.US);


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
