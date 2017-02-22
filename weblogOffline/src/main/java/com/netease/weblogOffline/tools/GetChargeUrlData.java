package com.netease.weblogOffline.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import com.netease.weblogCommon.data.enums.NeteaseContentType;
import com.netease.weblogCommon.utils.DateUtils;
import com.netease.weblogCommon.utils.UrlUtils;
import com.netease.weblogOffline.data.ContentScoreVector;

/**
 *	参数： 目录 近n天 （10）
 * */
public class GetChargeUrlData {
	
	private static final String hdfsDataDir = "/ntes_weblog/weblog/midLayer/contentScoreVector/";
	
	private static final String regex = "|*|";
	private static final String splitRegex = "#@@#";
	
	private static final String defaultVal = "0" + regex + "0" + regex + "0" + regex + "0" + regex + "0" + regex + "0";
	
	private static final List<String> vaildPrefixs = new ArrayList<String>();
	static {
		vaildPrefixs.add("http://help.3g.163.com");
		vaildPrefixs.add("http://3g.163.com");
	}
	private static boolean isValidUrl(String url){
		for(String prefix : vaildPrefixs){
			if(url.startsWith(prefix)){
				return false;
			}
		}
		
		return true;
	}
	
	private static void cleanUrl(String inputFile, String outputFile){
		BufferedReader reader = null;
		BufferedWriter writer = null;
		BufferedWriter invalidUrlWriter = null;
		try {
			reader = new BufferedReader(new FileReader(new File(inputFile)));
			writer = new BufferedWriter(new FileWriter(new File(outputFile)));
			String line = null;
			while(null != (line = reader.readLine())){
				try {
					String[] strs = line.split(splitRegex);
					String impureUrl = strs[strs.length - 1];
					String url = UrlUtils.getOriginalUrl(impureUrl.trim()).trim();
					StringBuilder sb = new StringBuilder();
					for(int i = 0; i < strs.length - 1; ++i){
						sb.append(strs[i]).append(regex);
					}
					sb.append(url);
					if(NeteaseContentType.artical.match(url) && isValidUrl(url)){
						String ct = UrlUtils.getAticalCt(url);
						writer.write(sb.toString() + regex + ct);
						writer.newLine();
					}else{
						if(null == invalidUrlWriter){
							int lastIndex = outputFile.lastIndexOf("/");
							String invalidUrlFile = outputFile.substring(0, lastIndex) + "/invalidUrls.tmp";
							invalidUrlWriter = new BufferedWriter(new FileWriter(new File(invalidUrlFile)));
						}
						invalidUrlWriter.write(line);
						invalidUrlWriter.newLine();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
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
			try {
				if(null != invalidUrlWriter){
					invalidUrlWriter.flush();
					invalidUrlWriter.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	@SuppressWarnings("deprecation")
	private static void getData(String inputFile, String outputFile, int n){
		Map<String, List<String>> validUrlsMap = new TreeMap<String, List<String>>();
		
		//read url
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(new File(inputFile)));
			String line = null;
			while(null != (line = reader.readLine())){
				try {
					String[] strs = line.split(splitRegex);
					int len = strs.length;
					if(len >= 2){
						String url = strs[len - 2];
						String ct = strs[len - 1];
						String toDt = DateUtils.getTheDayBefore(ct, -(n - 1));
						List<String> latestDateList = DateUtils.getDateList(ct, toDt);
						for(String dt : latestDateList){
							List<String> list = validUrlsMap.get(dt);
							if(null == list){
								list = new ArrayList<String>();
								validUrlsMap.put(dt, list);
							}
							list.add(url);
						}
					}else{
						System.out.println("getData error split: " + line);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		//get data
		BufferedWriter writer = null;
		try {
			FileSystem hdfs = FileSystem.get(new Configuration());
			writer = new BufferedWriter(new FileWriter(new File(outputFile)));
			SequenceFile.Reader hdfsReader = null;
	        Text key = new Text();
	        ContentScoreVector value = new ContentScoreVector();
	        
			for(String dt : validUrlsMap.keySet()){
				System.out.println("process " + dt);
				String dirStr = hdfsDataDir + dt + "/";
				Path dir = new Path(dirStr);
				
				if(!hdfs.exists(dir)){
					System.out.println(dirStr + " is not exists");
					continue;
				}
				
				List<String> validUrlList = validUrlsMap.get(dt);
				
				for(FileStatus status : hdfs.listStatus(dir)){
					if(status.getPath().getName().startsWith("p")){
						try {
							hdfsReader = new SequenceFile.Reader(hdfs, status.getPath(), new Configuration());
							while (hdfsReader.next(key, value)) {
								String url = key.toString();
								if(null != validUrlList && validUrlList.contains(url)){
									writer.write(url + regex + dt + regex + value.getPv() + regex + value.getUv() + regex + value.getGenTieCount() 
											+ regex + value.getGenTieUv() + regex + value.getShareCount() + regex + value.getBackCount());
									writer.newLine();
								}
							}
						} catch (Exception e) {
							e.printStackTrace();
						}finally{
							try {
								hdfsReader.close();
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			try {
				writer.flush();
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	private static void addAndMerge(String contentFile, String dataFile, String outputFile){
		//add
		BufferedReader dataFileReader = null;
		BufferedReader contentFileReader = null;
		BufferedWriter writer = null;
		try {
			Map<String, int[]> dataMap = new HashMap<String, int[]>();
			
			dataFileReader = new BufferedReader(new FileReader(new File(dataFile)));

			String line = null;
			while(null != (line = dataFileReader.readLine())){
				try {
					String[] strs = line.split(splitRegex);
					if(strs.length == 8){
						String url = strs[0];
						String dt = strs[1];
						int pv = Integer.parseInt(strs[2]);
						int	uv = Integer.parseInt(strs[3]);
						int	genTieCount = Integer.parseInt(strs[4]);
						int	genTieUv = Integer.parseInt(strs[5]);
						int	shareCount = Integer.parseInt(strs[6]);
						int	backCount = Integer.parseInt(strs[7]);
						
						int[] values = dataMap.get(url);
						if(null == values){
							values = new int[]{0,0,0,0,0,0};
							dataMap.put(url, values);
						}
						values[0] += pv;
						values[1] += uv;
						values[2] += genTieCount;
						values[3] += genTieUv;
						values[4] += shareCount;
						values[5] += backCount;
					}else{
						System.out.println("add error split: " + line);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
			
			contentFileReader = new BufferedReader(new FileReader(new File(contentFile)));
			writer = new BufferedWriter(new FileWriter(new File(outputFile)));
			
			line = null;
			while(null != (line = contentFileReader.readLine())){
				try {
					String[] strs = line.split(splitRegex);
					int len = strs.length;
					if(len > 2){
						String url = strs[len - 2];
						int[] values = dataMap.get(url);
						String resStr = defaultVal;
						
						if(null != values){
							StringBuilder sb = new StringBuilder();
							for(int val : values){
								sb.append(val).append(regex);
							}

							resStr = sb.toString();
						}
						
						writer.write(line + regex + resStr);
						writer.newLine();
					}else{
						System.out.println("merge error split: " + line);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				dataFileReader.close();
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
	
	/**
	 * 参数：1. 指定目录，该目录下有个urls文件，内容有 王彬 给出
	 *     2. 指定天数n，每天文章取自发布起n天内的数据，一般n=10
	 * */
	public static void main(String[] args) {
		if(args.length < 2){
			System.out.println("args error: dir n");
			return;
		}
		
		String dir = args[0].endsWith("/") ? args[0] : args[0] + "/";
		String input = dir + "urls";
		String output = dir + "result";
		String pureUrlTmpFile = dir + "pureUrl.tmp";
		String allDataTmpFile =  dir + "allData.tmp";
		
		int n = Integer.parseInt(args[1]);
		
		if(args.length >= 3){
			if("step1".equals(args[2])){
				cleanUrl(input, pureUrlTmpFile);
			}else if("step2".equals(args[2])){
				getData(pureUrlTmpFile, allDataTmpFile, n);
			}else if("step3".equals(args[2])){
				addAndMerge(pureUrlTmpFile, allDataTmpFile, output);
			}else{
				cleanUrl(input, pureUrlTmpFile);
				getData(pureUrlTmpFile, allDataTmpFile, n);
				addAndMerge(pureUrlTmpFile, allDataTmpFile, output);
			}
		}else{
			cleanUrl(input, pureUrlTmpFile);
			getData(pureUrlTmpFile, allDataTmpFile, n);
			addAndMerge(pureUrlTmpFile, allDataTmpFile, output);
		}
		
		System.out.println("finish");
	}
}
