package com.netease.weblogOffline.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import com.netease.weblogCommon.data.enums.NeteaseContentType;
import com.netease.weblogCommon.utils.UrlUtils;

public class ChargeUrlFilter {
	private static String originUrls = "C:/Users/Administrator/Desktop/data/chargeUrl/originUrls";
	private static String pureUrls = "C:/Users/Administrator/Desktop/data/chargeUrl/pureUrls";
	
	public static void main(String[] args) {
		BufferedReader reader = null;
		BufferedWriter writer = null;
		try {
			reader = new BufferedReader(new FileReader(new File(originUrls)));
			writer = new BufferedWriter(new FileWriter(new File(pureUrls)));
			
			String line = null;
			
			while(null != (line = reader.readLine())){
				String url = UrlUtils.getOriginalUrl(line);
				if(NeteaseContentType.artical.match(url)){
					writer.write(url.trim());
					writer.newLine();
				}else{
					System.out.println(url);
				}
			}
			
			System.out.println("finish");
			System.exit(0);
		}catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				writer.flush();
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
}
