/**
 * @(#)FileFormater.java, 2012-11-22. 
 * 
 * Copyright 2012 Netease, Inc. All rights reserved.
 * NETEASE PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.netease.weblogCommon.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
* this is used for mr filename
* @author mengyan
*
*/
public class FileFormater {
    
   private static GregorianCalendar calendar = new GregorianCalendar();
    
    private static String fileNameFormat = "yyyyMMdd";
    
    public static final String YEAR_MONTH_DAY = "yyyyMMdd";

    public static final String YEAR_MONTH_DAY_HOUR = "yyyyMMddHH";

    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat(fileNameFormat);
    
    public static void setDateFormat(String myFileNameFormat) {
        fileNameFormat = myFileNameFormat;
        simpleDateFormat = new SimpleDateFormat(fileNameFormat);
    }
    
    public static String getCurFileName() {
        return simpleDateFormat.format(new Date());
    }
    
    
    public static String getLastFileName() {
        calendar.setTime(new Date());
        if (fileNameFormat.equals(YEAR_MONTH_DAY)) {
            calendar.add(Calendar.DAY_OF_MONTH, -1);
        } else if (fileNameFormat.equals(YEAR_MONTH_DAY_HOUR)) {
            calendar.add(Calendar.HOUR_OF_DAY, -1);
        } else {
            calendar.add(Calendar.DAY_OF_MONTH, -1);
        }
        return simpleDateFormat.format(calendar.getTime());
    }
    
    public static String getPreFileName(Date date, int minus) {
        calendar.setTime(date);
        if (fileNameFormat.equals(YEAR_MONTH_DAY)) {
            calendar.add(Calendar.DAY_OF_MONTH, -minus);
        } else if (fileNameFormat.equals(YEAR_MONTH_DAY_HOUR)) {
            calendar.add(Calendar.HOUR_OF_DAY, -minus);
        } else {
            calendar.add(Calendar.DAY_OF_MONTH, -minus);
        }
        return simpleDateFormat.format(calendar.getTime());
    }
    
    public static String getPreFileName(int minus) {
        calendar.setTime(new Date());
        if (fileNameFormat.equals(YEAR_MONTH_DAY)) {
            calendar.add(Calendar.DAY_OF_MONTH, -minus);
        } else if (fileNameFormat.equals(YEAR_MONTH_DAY_HOUR)) {
            calendar.add(Calendar.HOUR_OF_DAY, -minus);
        } else {
            calendar.add(Calendar.DAY_OF_MONTH, -minus);
        }
        return simpleDateFormat.format(calendar.getTime());
    }
    
    public static String format(Date now) {
        return simpleDateFormat.format(now);
    }
    
    public static Date getDate(String now) {
        try {
            return simpleDateFormat.parse(now);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }
    
    public static void main(String[] args) {
        FileFormater.setDateFormat(YEAR_MONTH_DAY);
        System.out.println(FileFormater.getLastFileName());
        System.out.println(FileFormater.getPreFileName(23));
        FileFormater.setDateFormat(YEAR_MONTH_DAY_HOUR);
        System.out.println(FileFormater.getLastFileName());
        System.out.println(FileFormater.getPreFileName(12));
    }
    
}

