package com.netease.weblogCommon.utils;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.commons.lang3.StringUtils;

/**
 * Text utils 
 * @author mingli
 *
 */
public class TextUtils {
	private static ScriptEngineManager manager = new ScriptEngineManager();
	private static ScriptEngine engine = manager.getEngineByName("JavaScript");
	
	public static void main(String[] args) {
		try {
//			String s = "cdata={%22text%22:%22%E6%B5%99%E6%B1%9F%E8%8B%8D%E5%8D%97%E4%B8%80%E6%8B%93%E5%B1%95%E5%9F%BA%E5%9C%B0%E8%AE%AD%E7%BB%83%E7%BD%91%E5%9D%8D%E5%A1%8C%20%E5%A4%9A%E4%BA%BA%E6%AD%BB%E4%BC%A4%22}";
			String s = "http%3A//snstj.auto.163.com/15/1030/16/B76JLTB400084K7M_all.html%3Fsnstj_weixin";
			String s1 = unescape(s);
			String s2 = new String(s1.getBytes("GBK"),"UTF-8");
			String s3 = new String(s1.getBytes("ISO-8859-1"),"UTF-8");
			System.out.println(s1);
			System.out.println(s2);
			System.out.println(s3);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(0);
	}
	
	
	/**
	 * this method is to execute the "unescape" method in js
	 * @param value
	 * @return
	 */
	public static String unescape(String value) {
		String ret = "";
		if (value == null) {
			return null;
		}
		try {
			String oneLog = value.replaceAll("\"", "%22");//日志预处理
			ret =  engine.eval("unescape(\""+oneLog+"\")").toString();
		} catch (ScriptException e) {
			ret = value;
		}
		return ret;		
	}
	
	public static String notNullStr(String s, String def) {
		return StringUtils.isBlank(s) ? def : s.trim();
	}
	
	public static String notNullStr(String s) {
		return notNullStr(s, "");
	}
	
    /**
     * 加强版 first.compareTo(second)
     * */
    public static int strCompare(String first, String second){
    	if(null != first && null != second){
    		return first.compareTo(second);
    	}else if (null == first && null != second) {
    		return -1;
    	}else if (null != first && null == second ){
    		return 1;
    	}else{
    		return 0;
    	}
    }
    
    public static String quickUnescape(String src) {
		if(StringUtils.isBlank(src)){
			return src;	
		}
		
        try {
        	src = src.replaceAll("\"", "%22");//日志预处理
        	src = src.replace("\\x", "%");
        	
			StringBuffer tmp = new StringBuffer();  
			tmp.ensureCapacity(src.length());  
			int lastPos = 0, pos = 0;  
			char ch;  
			while (lastPos < src.length()) {
			    pos = src.indexOf("%", lastPos);  
			    if (pos == lastPos && pos < src.length()) {
			    	int intFrom;
			    	int intTo;
			    	int len;
			    	
			        if (pos + 1 < src.length() && src.charAt(pos + 1) == 'u') {
			        	intFrom = pos + 2;
			        	intTo =  pos + 6;
			        	len = 6;
			        } else {  
			        	intFrom = pos + 1;
			        	intTo =  pos + 3;
			        	len = 3;
			        }  
			        
			        if(intFrom < src.length() && intTo <= src.length()){
			        	String intS = src.substring(intFrom, intTo);
			        	try {
							ch = (char) Integer.parseInt(intS, 16);
							tmp.append(ch);
						} catch (Exception e) {
							tmp.append("%");
							len = 1;
						}  
			        }else{
			        	tmp.append("%" + src.substring(pos, src.length()));
			        }
			        
		            lastPos = pos + len; 
			    } else {
			        if (pos == -1) {  
			            tmp.append(src.substring(lastPos));  
			            lastPos = src.length();  
			        } else {  
			            tmp.append(src.substring(lastPos, pos));  
			            lastPos = pos;  
			        }  
			    }  
			}  

			return tmp.toString();
		} catch (Exception e) {
			e.printStackTrace();
			return src;
		}  
    }  
}
