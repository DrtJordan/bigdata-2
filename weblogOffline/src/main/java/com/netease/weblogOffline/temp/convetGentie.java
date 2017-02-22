package com.netease.weblogOffline.temp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.HashMap;

import com.netease.weblogCommon.utils.UrlUtils;


public class convetGentie {
	public static void main(String[] args) throws ParseException, IOException {
		BufferedReader br1 = null;
		BufferedReader br2 = null;
		 BufferedWriter writer = null;
		File f = new File("C:\\Users\\liyonghong\\Downloads\\gongshao");
		File f2 = new File("C:\\Users\\liyonghong\\Downloads\\photo_pv_format.txt");
	    HashMap<String,String> hs = new HashMap<String,String>();
	    
			  br1 = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
			  
			  br2 = new BufferedReader(new InputStreamReader(new FileInputStream(f2)));
			  String str = null;
			  while ((str = br1.readLine())!=null){
					 try {
						 
						 String[] strs = str.split("\t");
						 hs.put(strs[1], strs[0]);
					
						 
					 }catch(Exception e ){
						
					 }

				 }
			  writer = new BufferedWriter(new FileWriter("C:\\Users\\liyonghong\\Downloads\\photo_pv_format2.txt")); 
		
			  while ((str = br2.readLine())!=null){
					 try {
						 
						 String[] strs2 = str.split("\t");
						 String pureurl = UrlUtils.getOriginalUrl(strs2[0]);
						 if(hs.get(pureurl)!=null){
								writer.write(hs.get(pureurl)+"\t"+strs2[0]+"\t"+strs2[1]);
								writer.newLine();  
						 }
						 
					 }catch(Exception e ){
						
					 }

				 }
			  
		

	
				 try {
						writer.flush();
						writer.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
			}
		
	}
	
}
