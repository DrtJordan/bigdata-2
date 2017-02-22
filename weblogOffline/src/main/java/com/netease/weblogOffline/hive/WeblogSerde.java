package com.netease.weblogOffline.hive;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.netease.weblogCommon.data.enums.NeteaseChannel_CS;
import com.netease.weblogCommon.data.enums.SimpleDateFormatEnum;
import com.netease.weblogCommon.utils.JsonUtils;
import com.netease.weblogCommon.utils.TextUtils;
import com.netease.weblogCommon.utils.UrlUtils;

@SuppressWarnings("deprecation")
public class WeblogSerde implements Deserializer {
	private static List<String> structFieldNames = new ArrayList<String>();
	private static List<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();

	static {
		structFieldNames.add("ip");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("serverTime");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("project");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("event");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("esource");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("einfo");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		
		structFieldNames.add("cost");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("uuid");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("url");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("pureUrl");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("contentChannel");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("urlDomain");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("ref");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("refDomain");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("pver");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("cdata");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("pagescroll");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("pagescrollx");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("pagescrolly");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		
		
		structFieldNames.add("sid");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("ptype");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("entry");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("pgr");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("prev_pgr");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("avlbsize");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("avlbsizex");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("avlbsizey");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		
		structFieldNames.add("resolution");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("r");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("ccount");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("ctotalcount");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("csource");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("unew");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("utime");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("uctime");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("ultime");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("email");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("cdata_pageX");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("cdata_pageY");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("cdata_time");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("cdata_button");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("cdata_tag");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("cdata_href");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("cdata_text");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("cdata_img");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("cdata_jcid");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("cdata_page");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("browserInfo");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		structFieldNames.add("extra_pageY");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		
		structFieldNames.add("extra_pageX");
		structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		
	}

	public Object deserialize(Writable writable) throws SerDeException {
		List<Object> result = new ArrayList<Object>();
		try {
			Text rowText = (Text) writable;
			String[] items = rowText.toString().split(" ");
			if(items != null && items.length > 0){
				result.add(items[0]);//ip
			}
			
			String serverTimeStr = items[1].trim();
			if(serverTimeStr.startsWith("[")){
				serverTimeStr = serverTimeStr.substring(1);
			}
			long serverTime = 0;
			try{
				serverTime = SimpleDateFormatEnum.weblogServerTimeFormat.get().parse(serverTimeStr).getTime();
			}catch(ParseException e){
				serverTime = 0;
			}
			
			result.add(0 == serverTime ? "null" : String.valueOf(serverTime));//serverTime
			
			String rowStr = "";
			StringBuilder browserInfoSB = new StringBuilder();
			boolean browserInfoBegin = false;
			for(String item : items){
			 	if(item.startsWith("/stat/?")){
			 		rowStr = item.replaceAll("/stat/\\?", "");
			 	}
			 	
			 	if(item.startsWith("HTTP")){
			 		browserInfoBegin = true;
			 	}
			 	if(browserInfoBegin && !item.startsWith("\"http:")
			 			&& !item.startsWith("http:")){
			 		browserInfoSB.append(item).append(" ");
			 	}
			}

			String[] splits = rowStr.split("&");
			Map<String, String> map = new HashMap<String, String>();
			for (String s : splits){
				try {
					int first = s.indexOf("=");
					if (first != -1){
						map.put(s.substring(0, first), s.substring(first + 1, s.length()));
					}
				} catch (Exception localException) {}
			}
			
			String project = stringProcess(map.get("project"));
			result.add(project);
			
			String event = stringProcess(map.get("event"));
			result.add(event);
			String esource = stringProcess(map.get("esource"));
			result.add(esource);
			String einfo = stringProcess(map.get("einfo"));
			result.add(einfo);
			
			String cost = stringProcess(map.get("cost"));
			result.add(cost);
			
			String uuid = stringProcess(map.get("uuid"));
			result.add(uuid);
			
			String url = stringProcess(map.get("url"));
			result.add(url);
			
			String pureUrl = UrlUtils.getOriginalUrl(url);
			result.add(pureUrl);
			
			String contentChannel = stringProcess(NeteaseChannel_CS.getChannelName(url));
			result.add(contentChannel);
			
			String urlDomain =  stringProcess(urlGetDomain(url));
			result.add(urlDomain);
			
			String ref = stringProcess(map.get("ref"));
			result.add(ref);
			
			String refDomain = urlGetDomain(ref);
			result.add(refDomain);
			
			String pver = stringProcess(map.get("pver"));
			result.add(pver);
			
			String cdata_origin = stringProcess(map.get("cdata"));
			String cdata = new String(cdata_origin.getBytes("ISO-8859-1"),"UTF-8");
			result.add(cdata);
			
			String pagescroll = stringProcess(map.get("pagescroll"));
			result.add(pagescroll);
			String pagescrollx="";
			try {
				 pagescrollx = pagescroll.split("/")[0];
			}catch (Exception e ){
				 pagescrollx ="";
			} 
		
			result.add(pagescrollx);
			
			String pagescrolly="";
			try {
				 pagescrolly = pagescroll.split("/")[1];
			}catch (Exception e ){
				 pagescrolly ="";
			}
			result.add(pagescrolly);
			
			String sid = stringProcess(map.get("sid"));
			result.add(sid);
			
			String ptype = stringProcess(map.get("ptype"));
			result.add(ptype);
			
			String entry = stringProcess(map.get("entry"));
			result.add(entry);
			
			String pgr = stringProcess(map.get("pgr"));
			result.add(pgr);
			
			String prev_pgr = stringProcess(map.get("prev_pgr"));
			result.add(prev_pgr);
			
			String avlbsize = stringProcess(map.get("avlbsize"));
			result.add(avlbsize);
			
			String avlbsizex ="";
			try {
				avlbsizex= avlbsize.split("/")[0];
			}catch(Exception e ){
				avlbsizex="";
			}
					
			result.add(avlbsizex);
			
			String avlbsizey ="";
			try {
				avlbsizey= avlbsize.split("/")[1];
			}catch(Exception e ){
				avlbsizey="";
			}
					
			result.add(avlbsizey);
			
			String resolution = stringProcess(map.get("resolution"));
			result.add(resolution);
			
			String r = stringProcess(map.get("r"));
			result.add(r);
			
			String ccount = stringProcess(map.get("ccount"));
			result.add(ccount);
			
			String ctotalcount = stringProcess(map.get("ctotalcount"));
			result.add(ctotalcount );
			
			String csource = stringProcess(map.get("csource"));
			result.add(csource);
			
			String unew = stringProcess(map.get("unew"));
			result.add(unew);
			
			String utime = stringProcess(map.get("utime"));
			result.add(utime);
			
			String uctime = stringProcess(map.get("uctime"));
			result.add(uctime);
			
			String ultime = stringProcess(map.get("ultime"));
			result.add(ultime);
				
			Map<String ,String> cvarMap = new HashMap<String, String>();
			try {
				String cvar = stringProcess(map.get("cvar"));
				if(cvar.length() > 2){
					Map<String, String> json2Map = JsonUtils.json2Map(cvar);
					if(null != json2Map){
						cvarMap.putAll(json2Map);
					}
				}
			} catch (Exception e) {}

			String email = stringProcess(cvarMap.get("email"));
			result.add(email);
			
			Map<String ,String> cdataMap = new HashMap<String, String>();
			try {
				if(cdata.length() > 2){
					Map<String, String> json2Map = JsonUtils.json2Map(cdata);
					if(null != json2Map){
						cdataMap.putAll(json2Map);
					}
				}
			} catch (Exception e) {}
			
			String cdata_pageX = stringProcess(cdataMap.get("pageX"));
			result.add(cdata_pageX);
			String cdata_pageY = stringProcess(cdataMap.get("pageY"));
			result.add(cdata_pageY);
			String cdata_time = stringProcess(cdataMap.get("time"));
			result.add(cdata_time);
			String cdata_button = stringProcess(cdataMap.get("button"));
			result.add(cdata_button);
			String cdata_tag = stringProcess(cdataMap.get("tag"));
			result.add(cdata_tag);
			String cdata_href = stringProcess(cdataMap.get("href"));
			result.add(cdata_href);
			String cdata_text = stringProcess(cdataMap.get("text"));
			result.add(cdata_text);
			String cdata_img = stringProcess(cdataMap.get("img"));
			result.add(cdata_img);
			String cdata_jcid = stringProcess(cdataMap.get("jcid"));
			result.add(cdata_jcid);
			String cdata_page = stringProcess(cdataMap.get("page"));
			result.add(cdata_page);
			result.add(browserInfoSB.toString().trim());
			
			String extra_pageY = "";
			if(StringUtils.isNotBlank(cdata)){
				try {
					String[] cdataArr = cdata.split(",");
					for(String s : cdataArr){
						if(s.replaceAll("\"", "").startsWith("pageY")){
							extra_pageY = s.split(":")[1];
							break;
						}
					}
				} catch (Exception e) {}
			}
			result.add(extra_pageY);
			
			String extra_pageX = "";
			if(StringUtils.isNotBlank(cdata)){
				try {
					String[] cdataArr = cdata.split(",");
					for(String s : cdataArr){
						if(s.replaceAll("\"", "").startsWith("{pageX")){
							extra_pageX = s.split(":")[1];
							break;
						}
					}
				} catch (Exception e) {}
			}
			result.add(extra_pageX);
		} catch (Exception localException1) {}
		
		return result;
	}
	
	private String stringProcess(String s) {
		String unescapeStr = TextUtils.quickUnescape(s);
		return (StringUtils.isBlank(unescapeStr) ? "" : unescapeStr.trim());
	}
	
	private String urlGetDomain(String url){
		if(StringUtils.isBlank(url)){
			return "";
		}
		String temp = url.replace("http://", "").replace("https://", "");
		int first = temp.indexOf("/");
		if(first != -1){
			return temp.substring(0, first);
		}else{
			return temp; 
		}
	}

	public ObjectInspector getObjectInspector() throws SerDeException {
		return ObjectInspectorFactory.getStandardStructObjectInspector(
				structFieldNames, structFieldObjectInspectors);
	}

	public SerDeStats getSerDeStats() {
		return null;
	}

	public void initialize(Configuration job, Properties arg1)
			throws SerDeException {
	}
}



