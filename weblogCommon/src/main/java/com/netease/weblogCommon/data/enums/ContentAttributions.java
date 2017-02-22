package com.netease.weblogCommon.data.enums;

import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;

import com.netease.weblogCommon.utils.DateUtils;


public enum ContentAttributions {
	
	id_3w("id_3w"),//3w: 文章:docid;专题:;图集:栏目id_图集id;视频:vid
	id_3g("id_3g"),//3g: 文章:docid;专题:专题id;图集:频道id_图集id;视频:vid
	source("source"),
	author("author"),
	isOriginal("isOriginal"),
	docid_3g("docid_3g"),
	title_3w("title_3w"),
	title_3g("title_3g"),
	topic_3w("topic_3w"),//栏目号 3w
	topic_3g("topic_3g"),//栏目号 3g
	url_3w("url_3w"),
	url_3g("url_3g"),
	type_3w("type_3w"),
	type_3g("type_3g"),
	channel_3w("channel_3w"),//频道号 3w
	channel_3g("channel_3g"),//频道号 3g
	editor_3w("editor_3w"),
	editor_3g("editor_3g"),
	publishTime_3w("publishTime_3w"),
	publishTime_3g("publishTime_3g"),
	activeFlag("activeFlag");//内容自发布之日起400天内的被访问情况
	
	private String name;
	private static String[] attributions;

	private ContentAttributions(String name) {
		this.name = name;
	}
	
	static{
		Set<String> set = new TreeSet<>();
		for(ContentAttributions attr : ContentAttributions.values()){
			set.add(attr.getName());
		}
		attributions = set.toArray(new String[0]);
	}
	
	public static String[] getAttributions(){
		return attributions;
	}
	
	public String getName(){
		return name;
	}
	
	private interface ActiveFlagProcessor{
		public static final int maxDays = 8 * 50;
		public String getDefaultActiveFlag();
		public String updateActiveFlag(String activeFlag, String publishDt, String activeDt) throws Exception;
		public int getActiveDayNum(String activeFlag, String publishDt, String fromDt, String toDt) throws Exception;
		public String getLastActiveDt(String activeFlag, String publishDt) throws Exception;
	}
	
	public static class FullStrProcessor implements ActiveFlagProcessor{
		private static final char activeVal = '1';
		private static final char notActiveVal = '0';
		
		private String getSameCharStr(char c, int size){
			StringBuilder sb = new StringBuilder();
			for(int i = 0; i < size; ++i){
				sb.append(notActiveVal);
			}
			return sb.toString();
		}
		
		@Override
		public String getDefaultActiveFlag(){
			return getSameCharStr(notActiveVal, maxDays);
		}
		
		@Override
		public String updateActiveFlag(String activeFlag, String publishDt, String activeDt) throws Exception{
			if(StringUtils.isBlank(activeFlag)){
				activeFlag = getDefaultActiveFlag();
			}
			
			int dateDistance = DateUtils.getDateDistance(publishDt, activeDt);
			String res = activeFlag;
			
			if(dateDistance >= maxDays){
				return res;
			}
			
			if(dateDistance == 0){
				res = activeVal + activeFlag.substring(1);
			}else if(dateDistance > 0 && dateDistance < maxDays - 1){
				res = activeFlag.substring(0, dateDistance) + activeVal + activeFlag.substring(dateDistance + 1);
			}else if(dateDistance == maxDays - 1){
				res = activeFlag.substring(0, dateDistance) + activeVal;
			}
			
			return res;
		}
		
		@Override
		public int getActiveDayNum(String activeFlag, String publishDt, String fromDt, String toDt) throws Exception{
			if(StringUtils.isBlank(activeFlag) || StringUtils.isBlank(fromDt) 
					|| StringUtils.isBlank(toDt) || StringUtils.isBlank(publishDt)){
				return 0;
			}
			
			int fromDtDistance = DateUtils.getDateDistance(publishDt,fromDt);
			int toDtDistance = DateUtils.getDateDistance(publishDt, toDt);
			
			if(fromDtDistance > toDtDistance || fromDtDistance > maxDays || toDtDistance < 0){
				return 0;
			}
			
			if(fromDtDistance < 0){
				fromDtDistance = 0;
			}
			
			if(toDtDistance >= maxDays){
				toDtDistance = maxDays - 1;
			}
			
			String activeStatus = activeFlag.substring(fromDtDistance, toDtDistance + 1);
			
			int res = 0;
			for(char c : activeStatus.toCharArray()){
				if(c == activeVal){
					res++;
				}
			}
			
			return res;
		}

		@Override
		public String getLastActiveDt(String activeFlag, String publishDt) throws Exception {
			int lastActiveDistance = 0;
			for(int i = 0; i < activeFlag.length(); ++i){
				if(activeVal == activeFlag.charAt(i)){
					lastActiveDistance = i;
				}
			}
			return DateUtils.getTheDayBefore(publishDt, -lastActiveDistance);
		}
	}
	
	public static class CompactStrProcessor implements ActiveFlagProcessor{
		
		private static final char activeVal = '1';
		private static final char notActiveVal = '0';
		
		private String getSameCharStr(char c, int size){
			StringBuilder sb = new StringBuilder();
			for(int i = 0; i < size; ++i){
				sb.append(notActiveVal);
			}
			return sb.toString();
		}
		
		@Override
		public String getDefaultActiveFlag(){
			return "";
		}
		
		@Override
		public String updateActiveFlag(String activeFlag, String publishDt, String activeDt) throws Exception{
			if(StringUtils.isBlank(activeFlag)){
				activeFlag = getDefaultActiveFlag();
			}else if(activeFlag.length() > maxDays){
				activeFlag = activeFlag.substring(0, maxDays);
			}
			
			int dateDistance = DateUtils.getDateDistance(publishDt, activeDt);
			String res = activeFlag;
			
			if(dateDistance >= maxDays){
				return res;
			}
			
			if(dateDistance == 0){
				res = String.valueOf(activeVal);
			}else if(dateDistance > 0 && dateDistance < activeFlag.length()){
				res = activeFlag.substring(0, dateDistance) + activeVal + activeFlag.substring(dateDistance + 1);
			}else if(dateDistance == activeFlag.length()){
				res = activeFlag.substring(0, dateDistance) + activeVal;
			}else if(dateDistance >= activeFlag.length()){
				res = activeFlag + getSameCharStr(notActiveVal, dateDistance - activeFlag.length()) + activeVal;
			}
			
			return res;
		}
		
		@Override
		public int getActiveDayNum(String activeFlag, String publishDt, String fromDt, String toDt) throws Exception{
			if(StringUtils.isBlank(activeFlag) || StringUtils.isBlank(fromDt) 
					|| StringUtils.isBlank(toDt) || StringUtils.isBlank(publishDt)){
				return 0;
			}
			
			int fromDtDistance = DateUtils.getDateDistance(publishDt,fromDt);
			int toDtDistance = DateUtils.getDateDistance(publishDt, toDt);
			
			int length = activeFlag.length();
			
			if(fromDtDistance > toDtDistance || fromDtDistance > length || toDtDistance < 0){
				return 0;
			}
			
			if(fromDtDistance < 0){
				fromDtDistance = 0;
			}
			
			if(toDtDistance >= length){
				toDtDistance = length - 1;
			}
			
			String activeStatus = activeFlag.substring(fromDtDistance, toDtDistance + 1);
			
			int res = 0;
			for(char c : activeStatus.toCharArray()){
				if(c == activeVal){
					res++;
				}
			}
			
			return res;
		}

		@Override
		public String getLastActiveDt(String activeFlag, String publishDt) throws Exception {
			int lastActiveDistance = 0;
			for(int i = 0; i < activeFlag.length(); ++i){
				if(activeVal == activeFlag.charAt(i)){
					lastActiveDistance = i;
				}
			}
			return DateUtils.getTheDayBefore(publishDt, -lastActiveDistance);
		}
	}
	
	public static class ActiveFlagUtils{
		
//		private static ActiveFlagProcessor processor = new FullStrProcessor();
		private static ActiveFlagProcessor processor = new CompactStrProcessor();
		
		public static String getDefaultActiveFlag(){
			return processor.getDefaultActiveFlag();
		}
		
		
		/**
		 * @param activeFlag
		 * @param publishDt yyyyMMdd
		 * @param activeDt yyyyMMdd
		 * @return
		 * @throws Exception
		 */
		public static String updateActiveFlag(String activeFlag, String publishDt, String activeDt) throws Exception{
			return processor.updateActiveFlag(activeFlag, publishDt, activeDt);
		}
		
		
		/**
		 * @param activeFlag
		 * @param publishDt yyyyMMdd
		 * @param fromDt yyyyMMdd
		 * @param toDt yyyyMMdd
		 * @return
		 * @throws Exception
		 */
		public static int getActiveDayNum(String activeFlag, String publishDt, String fromDt, String toDt) throws Exception{
			return processor.getActiveDayNum(activeFlag, publishDt, fromDt, toDt);
		}
		
		
		/**
		 * @param activeFlag
		 * @param publishDt yyyyMMdd
		 * @param resDateFormat
		 * @return yyyyMMdd
		 * @throws Exception
		 */
		public static String getLastActiveDt(String activeFlag, String publishDt) throws Exception{
			return processor.getLastActiveDt(activeFlag, publishDt);
		}
	}
	
	public static void main(String[] args) {
		try {
			String publishDt = "20151201";
			String af = ActiveFlagUtils.getDefaultActiveFlag();
			System.out.println("af=" + af + "\n");
		
			af = ActiveFlagUtils.updateActiveFlag(af, publishDt, "20151130");
			System.out.println("af=" + af + "\n");
			af = ActiveFlagUtils.updateActiveFlag(af, publishDt, "20151201");
			System.out.println("af=" + af + "\n");
			af = ActiveFlagUtils.updateActiveFlag(af, publishDt, "20151202");
			System.out.println("af=" + af + "\n");
			af = ActiveFlagUtils.updateActiveFlag(af, publishDt, "20151204");
			System.out.println("af=" + af + "\n");
			af = ActiveFlagUtils.updateActiveFlag(af, publishDt, "20151208");
			System.out.println("af=" + af + "\n");
			
			
			af = ActiveFlagUtils.updateActiveFlag(af, publishDt, "20151205");
			System.out.println("af=" + af + "\n");
			af = ActiveFlagUtils.updateActiveFlag(af, publishDt, "20151209");System.out.println(ActiveFlagUtils.getLastActiveDt(af, publishDt));
			System.out.println("af=" + af + "\n");
			af = ActiveFlagUtils.updateActiveFlag(af, publishDt, "20151210");System.out.println(ActiveFlagUtils.getLastActiveDt(af, publishDt));
			System.out.println("af=" + af + "\n");
			
//			System.out.println(ActiveFlagUtils.getActiveDayNum(af, publishDt, "20151101", "20151130"));//0
//			System.out.println(ActiveFlagUtils.getActiveDayNum(af, publishDt, "20151130", "20151202"));//2
//			System.out.println(ActiveFlagUtils.getActiveDayNum(af, publishDt, "20151202", "20151208"));//4
//			System.out.println(ActiveFlagUtils.getActiveDayNum(af, publishDt, "20151202", "20151218"));//4
//			System.out.println(ActiveFlagUtils.getActiveDayNum(af, publishDt, "20151209", "20151218"));//0
//			System.out.println(ActiveFlagUtils.getActiveDayNum(af, publishDt, "20151109", "20151218"));//5
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}









