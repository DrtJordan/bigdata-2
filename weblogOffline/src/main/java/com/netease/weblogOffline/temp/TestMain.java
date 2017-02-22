package com.netease.weblogOffline.temp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.netease.weblogCommon.utils.JsonUtils;
import com.netease.weblogCommon.utils.TextUtils;

public class TestMain extends TextUtils {
	public static void main(String[] args) {
		try {
			String in  = "C:/Users/Administrator/Desktop/编辑1.txt";
			BufferedReader r = new BufferedReader(new FileReader(new File(in)));
			String line = null;
			while((line = r.readLine()) != null){
				String s1 = unescape(line);
				String s2 = new String(s1.getBytes("GBK"),"UTF-8");
				String s3 = new String(s1.getBytes("ISO-8859-1"),"UTF-8");
				
				Map<String, String> json2Map = JsonUtils.json2Map(s3);
				
				System.out.println(s3);
				System.out.println(json2Map);
				
				String extra_pageY = "";
				if(StringUtils.isNotBlank(s1)){
					try {
						String[] cdataArr = s1.split(",");
						System.out.println(cdataArr.length);
						for(String s : cdataArr){
							s = s.replaceAll("\"", "");
							System.out.println("s=" + s);
							if(s.startsWith("pageY")){
								System.out.println("s=" + s);
								extra_pageY = s.split(":")[1];
								break;
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				
				System.out.println(extra_pageY);
			}
			
			r.close();
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
