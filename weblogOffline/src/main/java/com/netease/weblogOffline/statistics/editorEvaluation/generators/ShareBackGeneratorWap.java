package com.netease.weblogOffline.statistics.editorEvaluation.generators;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.netease.weblogCommon.data.enums.Platform;
import com.netease.weblogCommon.data.enums.ShareBackChannel_EE;
import com.netease.weblogCommon.data.enums.StatisticsIndicator;
import com.netease.weblogCommon.logparsers.LogParser;
import com.netease.weblogCommon.logparsers.ZyLogParams;
import com.netease.weblogCommon.logparsers.ZylogParser;
import com.netease.weblogCommon.tools.EditorEvaluationKeyBuilder;
import com.netease.weblogOffline.statistics.editorEvaluation.data.PvUvArgs;

/**
 * ymshang 20151109
 * sps(分享回流日志)日志解析。
 *
 **/
public class ShareBackGeneratorWap implements PvUvArgsGenerator {
	
	private LogParser parser = new ZylogParser(); 

	@Override
	public List<PvUvArgs> execute(String line) throws Exception {
		List<PvUvArgs> res = new ArrayList<PvUvArgs>();
		String id = ""; //文章id
		String spss = "";  //平台     spss=163（wap端）spss=light（特殊子项目，可忽略，弱新闻概念） spss=newsapp（移动客户端）
		Platform platform = null;  //平台
		String spst = "";  //内容分类 spst=0  文章页spst=1  动效专题（H5页面）spst=2  专题页 spst=3  图集页 spst=4  直播室

		Map<String, String> map = parser.parse(line);
		URL url = new URL(map.get(ZyLogParams.url));
		String query = url.getQuery();
		String []params = query.split("&");
		for(String param:params){
			if(param.split("=").length!=2){
				continue;
			}
			String key = param.split("=")[0];
			String value = param.split("=")[1];
			map.put(key,value);
		}
		spst = map.get("spst");
		if("0".equals(spst)){
			id = map.get("docid");
		}else if("1".equals(spst)){
			id = map.get("modelid");
		}else if("2".equals(spst)){
			id = map.get("sid");
		}else if("3".equals(spst)){
			id = map.get("setid");
		}else if("4".equals(spst)){
			id = map.get("roomid");
		}else if("6".equals(spst)){
			id = map.get("docid");
		}else{
			id = "";
		}
		spss = map.get("spss");
		if("163".equals(spss)){
			platform = Platform.wap;
		}else if("newsapp".equals(spss)){
			platform = Platform.app;
		}else {
			platform = Platform.other;
		}

		if("sharedone".equals(map.get("func"))){
			if(Platform.wap == platform){
				PvUvArgs cell = new PvUvArgs(id, map.get(ZyLogParams.uid),
						EditorEvaluationKeyBuilder.getColumnName(StatisticsIndicator.share, platform, ShareBackChannel_EE.all),
						1l);
				res.add(cell);
			}
		} else if (!map.containsKey("func")){
			PvUvArgs cell = new PvUvArgs(id, map.get(ZyLogParams.uid),
					EditorEvaluationKeyBuilder.getColumnName(StatisticsIndicator.back, platform, ShareBackChannel_EE.all),
					1l);
			res.add(cell);
		}
		return res;
	}
	public static void main(String args[]) throws Exception{
		String log = "2015-11-04 19:26:01\t6c74e19fc404ae65a25c5fd1b818a3ab\t0\t0\t0\t0\thttp%3A//sps.163.com/article/%3Fspst%3D0%26spss%3D163%26docid%3D6HJRQ6RQ00162OUT%26spsw%3D1%26spsf%3Dqq\t%u7F51%u6613%u65B0%u95FB\thttp%3A//m.sogou.com/web/searchList.js\n" +
				"p%3FuID%3DeX6NjQ-Mr1AchwF9%26v%3D5%26e%3D1427%26de%3D1%26pid%3Dsogou-clse-2996962656838a97%26dp%3D1%26w%3D1278%26t%3D1446635690777%26s_t%3D1446636335473%26keyword%3D%25E7%259B%259B%25E6%2599%25AF%25E7%25BD%2591%25E8%2581%2594%25E5%2595%2586%25E4%25B8%259A%25E6%25A8%25A1\n" +
				"%25E5%25BC%258F%26pg%3DwebSearchList\tMozilla/5.0 (Linux; U; Android 4.4.4; zh-cn; SM-N9109W Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko)Version/4.0 MQQBrowser/6.1 Mobile Safari/537.36\t1440x2560\tzh-cn\t24-bit\t1446607560\t101.226.61.186\t上海市\n" +
				"海市\t电信\t";
		Map<String, String> map = new ZylogParser().parse(log);
		String id = ""; //文章id

		String spss = "";  //平台     spss=163（wap端）spss=light（特殊子项目，可忽略，弱新闻概念） spss=newsapp（移动客户端）
		Platform platform = null;  //平台
		String spst = "";  //内容分类 spst=0  文章页spst=1  动效专题（H5页面）spst=2  专题页 spst=3  图集页 spst=4  直播室
		URL url = new URL(map.get(ZyLogParams.url));
		String query = url.getQuery();
		String []params = query.split("&");
		for(String param:params){
			if(param.split("=").length!=2){
				continue;
			}
			String key = param.split("=")[0];
			String value = param.split("=")[1];
			map.put(key,value);
		}
		spst = map.get("spst");
		if("0".equals(spst)){
			id = map.get("docid");
		}else if("1".equals(spst)){
			id = map.get("modelid");
		}else if("2".equals(spst)){
			id = map.get("sid");
		}else if("3".equals(spst)){
			id = map.get("setid");
		}else if("4".equals(spst)){
			id = map.get("roomid");
		}else{
			id = "";
		}
		spss = map.get("spss");
		if("163".equals(spss)){
			platform = Platform.wap;
		}else if("newsapp".equals(spss)){
			platform = Platform.app;
		}else {
			platform = Platform.other;
		}

		System.out.println(map);
	}
}
