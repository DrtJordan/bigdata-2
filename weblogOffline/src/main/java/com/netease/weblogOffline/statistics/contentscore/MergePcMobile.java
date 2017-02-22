package com.netease.weblogOffline.statistics.contentscore;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;


public class MergePcMobile {
	
	private static void merge(String pcResFile, String mobileResFile, String resultFile){
		try {
			File pcData = new File(pcResFile);
			File mobileData = new File(mobileResFile);
			File output = new File(resultFile);

			Map<String, String[]> pcDataMap = new HashMap<String, String[]>();
			Map<String, String[]> mobileDataMap = new HashMap<String, String[]>();
			
			//read pc data
			BufferedReader pcDataReader = new BufferedReader(new FileReader(pcData));
			String line = null;
			while((line = pcDataReader.readLine()) != null){
				try {
					String[] strs = line.split("\t");
					pcDataMap.put(strs[0], strs);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			try {
				pcDataReader.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			//read mobile data
			BufferedReader mobileDataReader = new BufferedReader(new FileReader(mobileData));
			line = null;
			while((line = mobileDataReader.readLine()) != null){
				try {
					String[] strs = line.split("\t");
					mobileDataMap.put(strs[0], strs);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			try {
				mobileDataReader.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			
			//merge pc and mobile
			BufferedWriter writer = new BufferedWriter(new FileWriter(output)); 
			for(Entry<String, String[]> entry : pcDataMap.entrySet()){
				StringBuilder sb = new StringBuilder();
				
				for(String s : entry.getValue()){
					sb.append(s).append("\t");
				}
				
				String[] md = mobileDataMap.get(entry.getKey());
				if(null != md && md.length >= 6){
					sb.append(md[md.length - 6]).append("\t").append(md[md.length - 5]).append("\t").append(md[md.length - 3]).append("\t").append(md[md.length - 2]);
				}else{
					sb.append("0\t0\t0\t0");
				}
				
				writer.write(sb.toString().trim());
				writer.newLine();
			}
			writer.flush();
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		if(args.length < 3){
			System.out.println("args error, args： pcResFile mobileResFile");
			return;
		}
		
		merge(args[0], args[1], args[2]);
	}
}
