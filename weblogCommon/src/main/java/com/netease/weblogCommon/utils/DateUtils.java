package com.netease.weblogCommon.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import com.netease.weblogCommon.data.enums.SimpleDateFormatEnum;
import com.netease.weblogCommon.data.enums.SortOrderEnum;

public class DateUtils {
	
	public static String dateFormatTransform(String src, SimpleDateFormat srcDateFormat, SimpleDateFormat destDateFormat){
		return getTime(toLongTime(src,srcDateFormat),destDateFormat);
	}
	
	public static String toStrTime(long ms, String format) {
		try {
			SimpleDateFormat dateFormat = new SimpleDateFormat(format);
			Date date = new Date();
			date.setTime(ms);

			return dateFormat.format(date);
		} catch (Exception e) {
			return null;
		}
	}
	
	
	
	public static long toLongTime(String time, SimpleDateFormat dateFormat) {
		try {
			Date date = dateFormat.parse(time);
			return date.getTime();
		} catch (Exception e) {
			return 0;
		}
	}
	
	public static long toLongTime(String time, String format) {
		try {
			SimpleDateFormat dateFormat = new SimpleDateFormat(format);
			return toLongTime(time, dateFormat);
		} catch (Exception e) {
			return 0;
		}
	}
	
    public static String getTime(long time, SimpleDateFormat format){
        Calendar calendar = GregorianCalendar.getInstance();
        calendar.setTimeInMillis(time);
        
        return format.format(calendar.getTime());
    }
    
    public static String getToday() {
    	return SimpleDateFormatEnum.dateFormat.get().format(new Date());
    }
    
    public static String getYesterday() {
        Calendar calendar = GregorianCalendar.getInstance();
        calendar.add(Calendar.DATE, -1);
        return SimpleDateFormatEnum.dateFormat.get().format(calendar.getTime());
    }
    
    /**
     * 输入日期为月底，返回true，否则返回false
     * */
    public static boolean runMonthlyJob(String date){
        Calendar today = Calendar.getInstance(); 
        
        if(null != date && !"".equals(date)){
            try {
                today.setTime(SimpleDateFormatEnum.dateFormat.get().parse(date));
                today.add(Calendar.DATE, 1);
            } catch (ParseException e) {
               return false;
            }
        }
        
        int d = today.get(Calendar.DAY_OF_MONTH);
        
        return d == 1;
    }
    /**
     * 输入日期为季度最后一天，返回true，否则返回false
     * */
    public static boolean runQuarterlyJob(String date){
        Calendar today = Calendar.getInstance();
        
        if(null != date && !"".equals(date)){
            try {
                today.setTime(SimpleDateFormatEnum.dateFormat.get().parse(date));
                today.add(Calendar.DATE, 1);
            } catch (ParseException e) {
                return false;
            }
        }
        
        int m = today.get(Calendar.MONTH);
        int d = today.get(Calendar.DAY_OF_MONTH);
        
        return d == 1 && m % 3 == 0;
    }
    
    public static List<String> getLatestDateList(String date, int n) throws ParseException {
    	return getLatestDateList(date, n, SortOrderEnum.desc);
    }
    
    public static List<String> getLatestDateList(String date, int n, SortOrderEnum order) throws ParseException {
        List<String> resultList = new ArrayList<String>();

        Date toDate = SimpleDateFormatEnum.dateFormat.get().parse(date);

        Calendar toGc = GregorianCalendar.getInstance();
        toGc.setTime(toDate);
        Calendar gc = GregorianCalendar.getInstance();
        gc.setTime(toDate);

        int count = 0;
        while (++count <= n) {
            Date temp = gc.getTime();
            resultList.add(SimpleDateFormatEnum.dateFormat.get().format(temp));
            gc.add(Calendar.DATE, -1);
        }

        if(SortOrderEnum.asc == order){
        	Collections.reverse(resultList);
        }
        
        return resultList;
    }
    
    /**返回输入日期的第前n天*/
    public static String getTheDayBefore(String date, int n) throws ParseException {
        Date inDate = SimpleDateFormatEnum.dateFormat.get().parse(date);
        
        Calendar calendar = GregorianCalendar.getInstance();
        calendar.setTime(inDate);
        calendar.add(Calendar.DATE, -n);

        return SimpleDateFormatEnum.dateFormat.get().format(calendar.getTime());
    }
    
    /**
     * 获取[from,to]之间的日期列表
     * @throws ParseException
     */
    public static List<String> getDateList(String from, String to) throws ParseException {
        List<String> resultList = new ArrayList<String>();
        
//        System.out.println("from=" + from + "\nto=" + to);

        Date fromDate = SimpleDateFormatEnum.dateFormat.get().parse(from);
        Date toDate = SimpleDateFormatEnum.dateFormat.get().parse(to);

        Calendar gc = GregorianCalendar.getInstance();
        gc.setTime(toDate);
        Calendar fromGc = GregorianCalendar.getInstance();
        fromGc.setTime(fromDate);

        while (gc.after(fromGc) || gc.equals(fromGc)) {
            Date temp = gc.getTime();
            resultList.add(SimpleDateFormatEnum.dateFormat.get().format(temp));
            gc.add(Calendar.DATE, -1);
        }

        return resultList;
    }
    
    /**
     * 获取输入日期当前月的所有日期
     * */
    public static List<String> getMonthDateList(String date) throws ParseException{
        
        Date dateTemp = SimpleDateFormatEnum.dateFormat.get().parse(date);
        
        Calendar from = GregorianCalendar.getInstance();
        from.setTime(dateTemp);
        from.set(Calendar.DAY_OF_MONTH, 1);
        
        Calendar to = GregorianCalendar.getInstance();
        to.setTime(dateTemp);
        int lastDay = to.getActualMaximum(Calendar.DAY_OF_MONTH);
        to.set(Calendar.DAY_OF_MONTH, lastDay);
        
        return getDateList(SimpleDateFormatEnum.dateFormat.get().format(from.getTime()), SimpleDateFormatEnum.dateFormat.get().format(to.getTime()));
    }
    
    /**
     * 获取输入日期当前季度的所有日期
     * */
    public static List<String> getQuarterDateList(String date) throws ParseException{
        Calendar from = GregorianCalendar.getInstance();
        from.setTime(SimpleDateFormatEnum.dateFormat.get().parse(date));
        Calendar to = GregorianCalendar.getInstance();
        to.setTime(SimpleDateFormatEnum.dateFormat.get().parse(date));
        
        int month = from.get(Calendar.MONTH);
        int quarter = month / 3;
        
        from.set(Calendar.MONTH, quarter * 3);
        from.set(Calendar.DATE, 1);
        to.set(Calendar.MONTH, (quarter *3 + 2));
        int lastDay = to.getActualMaximum(Calendar.DAY_OF_MONTH);
        to.set(Calendar.DAY_OF_MONTH, lastDay);
        
        return getDateList(SimpleDateFormatEnum.dateFormat.get().format(from.getTime()), SimpleDateFormatEnum.dateFormat.get().format(to.getTime()));
    }
    
    /**
     * 获取输入日期当前月的起始时间，包括
     * */
    public static long getMonthBeginTime(String date) throws ParseException{
        Date dateTemp = SimpleDateFormatEnum.dateFormat.get().parse(date);
        
        Calendar c = GregorianCalendar.getInstance();
        c.setTime(dateTemp);
        c.set(Calendar.DAY_OF_MONTH, 1);
        c.set(Calendar.HOUR, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        
        return c.getTimeInMillis();
    }
    
    /**
     * 获取输入日期当前月的结束时间，不包括
     * */
    public static long getMonthEndTime(String date) throws ParseException{
        Date dateTemp = SimpleDateFormatEnum.dateFormat.get().parse(date);
        
        Calendar c = GregorianCalendar.getInstance();
        c.setTime(dateTemp);
        c.add(Calendar.MONTH, 1);
        c.set(Calendar.DAY_OF_MONTH, 1);
        c.set(Calendar.HOUR, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        
        return c.getTimeInMillis();
    }
    
    public static long getQuarterBeginTime(String date) throws ParseException{
        
        Calendar c = GregorianCalendar.getInstance();
        c.setTime(SimpleDateFormatEnum.dateFormat.get().parse(date));
        
        int month = c.get(Calendar.MONTH);
        int quarter = month / 3;
        
        c.set(Calendar.MONTH, quarter * 3);
        c.set(Calendar.DATE, 1);
        c.set(Calendar.HOUR, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        
        return c.getTimeInMillis();
    }
    
    public static long getQuarterEndTime(String date) throws ParseException{
        
        Calendar c = GregorianCalendar.getInstance();
        c.setTime(SimpleDateFormatEnum.dateFormat.get().parse(date));
        
        int month = c.get(Calendar.MONTH);
        int quarter = month / 3;
        
        c.set(Calendar.MONTH, quarter * 3);
        c.add(Calendar.MONTH, 3);
        c.set(Calendar.DATE, 1);
        c.set(Calendar.HOUR, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        
        return c.getTimeInMillis();
    }
    
    
    /**
     * 获得当前月份上个月份的开始结束日期时间
     * @return
     */
     @SuppressWarnings("deprecation")
	public static Date[] getLastMonthStartEndDate(){
     	//处理当前日期
 		Calendar cc = Calendar.getInstance();
 		return getMonthStartEndDate((cc.getTime().getYear()+1900)+"", cc.getTime().getMonth()+"");
     }
     
     /**
      * 获得当前月份上个季度的开始结束日期时间
      * @return
      */
     @SuppressWarnings("deprecation")
	public static Date[] getLastQuartStartEndDate(){
     	//处理当前季度
 		Calendar cc = Calendar.getInstance();
 		String index = Math.round(Math.ceil((cc.getTime().getMonth()+1)/3)) + "";
 		return getQuartStartEndDate((cc.getTime().getYear()+1900) +"", index);
     }
 	
 	
     /**
      * 获得指定日期的起始结束日期
      * @param defDate
      * @return
      */
     @SuppressWarnings("deprecation")
	public static Date[] getStartEndDate(String defDate){
     	if(defDate.indexOf("M")==0){
     		//处理月份
     		if(defDate.indexOf("-")==1){
     			String temp = defDate.substring(2,defDate.length());
         		String[] array = temp.split("-");
         		if(array!=null && array.length==2){
         			return getMonthStartEndDate(array[0], array[1]);
         		} else {
         			return null;
         		}
     		} else if(defDate.toLowerCase().indexOf("last")==1){
     			//处理当前日期
     			Calendar cc = Calendar.getInstance();
     			return getMonthStartEndDate((cc.getTime().getYear()+1900)+"", cc.getTime().getMonth()+"");
     		}
     		return null;
     	} else if(defDate.indexOf("Q")==0){
     		//处理季度
     		if(defDate.indexOf("-")==1){
 	    		String temp = defDate.substring(2,defDate.length());
 	    		String[] array = temp.split("-");
 	    		if(array!=null && array.length==2){
 	    			return getQuartStartEndDate(array[0], array[1]);
 	    		} else {
 	    			return null;
 	    		}
     		}else if(defDate.toLowerCase().indexOf("last")==1){
     			//处理当前季度
     			Calendar cc = Calendar.getInstance();
     			String index = Math.round(Math.ceil((cc.getTime().getMonth()+1)/3)) + "";
     			return getQuartStartEndDate((cc.getTime().getYear()+1900) +"", index);
     		}
     		return null;
     	} else {
     		return null;
     	}
     }
     
     /**
      * 获得指定季度的起始日期时间及结束日期时间
      * @param year
      * @param month
      * @return
      */
     private static Date[] getQuartStartEndDate(String year, String quart){
     	try{
     		String[] startEndMonth = getQuartStartEndDate(quart);
     		if(startEndMonth==null ||startEndMonth.length!=2){
     			return null;
     		}
     		Date startDate = getMonthStartDate(year, startEndMonth[0]);
     		if(startDate==null){
     			return null;
     		}
     		Date endDate = getMonthEndDate(year, startEndMonth[1]);
     		if(endDate!=null){
     			Date[] array = {startDate,endDate};
     			return array;
     		}
     		
     	} catch(Exception e){
     		return null;
     	}
     	return null;
     }
     
     
     /**
      * 获得指定月份的起始日期时间及结束日期时间
      * @param year
      * @param month
      * @return
      */
     private static Date[] getMonthStartEndDate(String year, String month){
     	try{
     		Date startDate = getMonthStartDate(year, month);
     		if(startDate==null){
     			return null;
     		}
     		Date endDate = getMonthEndDate(year, month);
     		if(endDate!=null){
     			Date[] array = {startDate,endDate};
     			return array;
     		}
     		
     	} catch(Exception e){
     		return null;
     	}
     	return null;
     }
     
     /**
      * 获得指定季度的起始月份、结束月份
      * @param month
      * @return
      */
     private static String[] getQuartStartEndDate(String quart) {
     	try{
     		int quarter = Integer.parseInt(quart);
     		if(quarter<1||quarter>4){
     			return null;
     		}
     		return new String[]{quarter*3-2 + "", quarter*3 + ""};
     	} catch(Exception e){
     		return null;
     	}
     }  
     
     /**
      * 获得指定月份的月初日期及时间
      * @param year
      * @param month
      * @return
      */
     private static Date getMonthStartDate(String year, String month){
     	try{
     		Calendar calendar = new GregorianCalendar();
     		calendar.set(Calendar.YEAR, Integer.parseInt(year));
     		calendar.set(Calendar.MONTH, Integer.parseInt(month)-1);
     		calendar.set(Calendar.DAY_OF_MONTH,1);
     		calendar.set(Calendar.HOUR_OF_DAY, 0);
     		calendar.set(Calendar.MINUTE, 0);
     		calendar.set(Calendar.SECOND, 0);
     		return calendar.getTime();
     	} catch(Exception e){
     		return null;
     	}
     	
     }
     
     
     /**
      * 获得指定月份对应的月末日期及时间
      * @param year
      * @param month
      * @return
      */
     private static Date getMonthEndDate(String year, String month){
     	try{
     		Calendar calendar = new GregorianCalendar();
     		calendar.set(Calendar.YEAR, Integer.parseInt(year));
     		calendar.set(Calendar.MONTH, Integer.parseInt(month));
     		calendar.set(Calendar.DAY_OF_MONTH, 0);
     		calendar.set(Calendar.HOUR_OF_DAY, 23);
     		calendar.set(Calendar.MINUTE, 59);
     		calendar.set(Calendar.SECOND, 59);
     		return calendar.getTime();
     	} catch(Exception e){
     		return null;
     	}
     	
     }
     
     /**
      * 获得指定日期开始时间
      * @param year
      * @param month
      * @return
      */
     public static long getDayStartTime(String date,String dateFormat){
     	try{
     		Date dateTemp = new SimpleDateFormat(dateFormat).parse(date);
            
            Calendar c = GregorianCalendar.getInstance();
            c.setTime(dateTemp);
            c.set(Calendar.HOUR_OF_DAY, 0);
            c.set(Calendar.MINUTE, 0);
            c.set(Calendar.SECOND, 0);
            c.set(Calendar.MILLISECOND, 0);
            
            return c.getTimeInMillis();
     	} catch(Exception e){
     		return 0;
     	}
     	
     }
     
     
     /**
      * 获得指定日期结束时间
      * @param year
      * @param month
      * @return
      */
     public static long getDayEndTime(String date,String dateFormat){
     	try{
     		Date dateTemp = new SimpleDateFormat(dateFormat).parse(date);
            
            Calendar c = GregorianCalendar.getInstance();
            c.setTime(dateTemp);
            c.set(Calendar.HOUR_OF_DAY, 23);
            c.set(Calendar.MINUTE, 59);
            c.set(Calendar.SECOND, 59);
            c.set(Calendar.MILLISECOND, 999);
            return c.getTimeInMillis();
     	} catch(Exception e){
     		return 0;
     	} 
     	
     }
     
    public final static long oneDay = 1000 * 60 * 60 * 24; 
    
    public static int getDateDistance(String firstDt,String secondDt) {
    	return getDateDistance(firstDt, "yyyyMMdd", secondDt, "yyyyMMdd");
    }

	public static int getDateDistance(String firstDt, String firstDateFormat,String secondDt,String secondDateFormat) {
		long first = getDayStartTime(firstDt,firstDateFormat);
	//	long second = getDayStartTime(secondDt,"yyyy-MM-dd HH:mm:ss");
		long second = getDayStartTime(secondDt,secondDateFormat);
		int dist = (int) ((second - first) / oneDay);
		return dist;
	}
	
	public static void main(String[] args) {
		System.out.println(getDateDistance("20140802","yyyyMMdd","20150802","yyyyMMdd"));
	}

}

