package com.netease.weblogOffline.statistics.tokaola;


import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.netease.weblogCommon.logparsers.LogParser;
import com.netease.weblogCommon.utils.JsonUtils;
import com.netease.weblogCommon.utils.TextUtils;



public class NavlogParser implements LogParser{

  private static Logger LOG = Logger.getLogger(NavlogParser.class);
  private static SimpleDateFormat nginxSimpleDateFormat = SimpleDateFormatEnum.ngnixServerTimeFormat.get();
  private static SimpleDateFormat logSimpleDateFormat = SimpleDateFormatEnum.logTimeFormat.get();
  private static Pattern pattern = Pattern.compile("^/\\w+/\\?");
  //提取"安全退出"标签
  private static Pattern pattern_logout = Pattern.compile("Logout");
  //提取"频道分类"标签 
  private static Pattern pattern_channel = Pattern.compile("^#brief/\\d+/(card|list)$");
 

  public Map<String, String> parse(String line) {
    Map<String, String> resMap = new HashMap<String, String>();

    try {
      // 4个空格分隔
      String[] items = line.toString().split("    ");
      if (items != null && items.length == 5) {
        // 第三个字段 requst参数串
        String req = items[2];
        if (req.startsWith("[") && req.endsWith("]")) {
          String[] reqs = req.split(" ");
          // 截取 类似 /hot/? 后面的参数
          String param = "";
          for (String s : reqs) {
            if (pattern.matcher(s).find()) {
              param = s.substring(s.indexOf("?") + 1, s.length());
            }
          }
          String[] params = param.split("&");
          Map<String, String> map = new HashMap<String, String>();
          for (String p : params) {
            try {
              int first = p.indexOf("=");
              if (first != -1) {
                map.put(p.substring(0, first), stringProcess(p.substring(first + 1, p.length())));
              }
            } catch (Exception localException) {
            }
          }
          resMap.putAll(map);

          // 处理category
          if (StringUtils.isNotEmpty(map.get("platform"))) {
            resMap.put("category", map.get("platform"));
          } else {
            resMap.put("category", "nocategory");
          }

          // 处理cdata数据
          String cdata_origin = map.get("cdata");
			if (StringUtils.isNotEmpty(cdata_origin)&& !"null".equals(cdata_origin.toLowerCase())) {
				String cdata = null;
				try {
					cdata = new String(cdata_origin.getBytes("ISO-8859-1"),"UTF-8");
				} catch (UnsupportedEncodingException e) {
				}
				try {
					if (cdata != null && cdata.length() > 2) {
						resMap.put("cdata", cdata);
						Map<String, String> json2Map = JsonUtils.json2Map(cdata);
						if (null != json2Map) {
							for (Map.Entry<String, String> entry : json2Map.entrySet()) {

								// 得到cdata中的href标签
								if (stringProcess(entry.getKey()).equals("href"))
								{
									// href为"安全退出"
									if (pattern_logout.matcher(entry.getValue()).find())
									{
										resMap.put("action", "logout");
									}
									// 获取频道分类id
									if (pattern_channel.matcher(entry.getValue()).find())
									{
										String channelurl[] = stringProcess(entry.getValue()).split("/");

										boolean num = channelurl[1].matches("^[-+]?(([0-9]+)?)$");
										if (num)
											resMap.put("channel",channelurl[1]);
									}
								}
								resMap.put("cdata_" + entry.getKey(),stringProcess(entry.getValue()));
							}
						}

					}
				} catch (Exception e) {
				}
          }

        }

        // 第一个字段 ip
        resMap.put("remote_ip", items[0].trim());

        // 第二个字段 时间
        String serverTimeStr = items[1].trim();
        if (serverTimeStr.startsWith("[") && serverTimeStr.endsWith("]")) {
          serverTimeStr = serverTimeStr.substring(1, serverTimeStr.length() - 1);
        }
        try {
          resMap.put("server_time", logSimpleDateFormat.format(nginxSimpleDateFormat.parse(serverTimeStr)));
        } catch (ParseException e) {
        }

        // 第四个字段refer
        String http_referer = stringProcess(items[3]);
        if (http_referer.startsWith("\"") && http_referer.endsWith("\"")) {
          http_referer = http_referer.substring(1, http_referer.length() - 1);
        }
        resMap.put("http_referer", http_referer);

        // 第五个字段 user_agent
        String http_user_agent = items[4].trim();
        if (http_user_agent.startsWith("\"") && http_user_agent.endsWith("\"")) {
          http_user_agent = http_user_agent.substring(1, http_user_agent.length() - 1);
        }
        resMap.put("http_user_agent", http_user_agent);
      }
    } catch (Exception e) {
    	 LOG.error(line, e);
    }
    return resMap;
  }

  

  private static String stringProcess(String s) {
    String unescapeStr = TextUtils.quickUnescape(s);
    return (StringUtils.isBlank(unescapeStr) ? "" : unescapeStr.trim());
  }


}
