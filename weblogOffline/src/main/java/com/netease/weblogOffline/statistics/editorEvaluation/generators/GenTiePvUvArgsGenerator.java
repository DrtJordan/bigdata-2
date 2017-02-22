package com.netease.weblogOffline.statistics.editorEvaluation.generators;

import java.util.ArrayList;
import java.util.List;

import com.netease.weblogCommon.data.enums.Platform;
import com.netease.weblogCommon.data.enums.ShareBackChannel_EE;
import com.netease.weblogCommon.data.enums.StatisticsIndicator;
import com.netease.weblogCommon.tools.EditorEvaluationKeyBuilder;
import com.netease.weblogOffline.statistics.editorEvaluation.data.PvUvArgs;

public class GenTiePvUvArgsGenerator implements PvUvArgsGenerator {

	@Override
	public List<PvUvArgs> execute(String line) throws Exception {
//		url,pdocid,docid,发帖用户id,跟帖时间,跟帖id,ip,source
		String[] strs = line.split(",");
		String url = strs[0];
		String docid = strs[2];
    	String uid = strs[3];
    	String source = strs[7];
    	String key = docid+","+url;
    	Platform platform = null;
    	
    	if("ph".equals(source)){
    		platform = Platform.app;
    	}else if("wb".equals(source)){
    		platform = Platform.www;
    	}else if("3g".equals(source)){
    		platform = Platform.wap;
    	}else{
    		platform = Platform.other;
    	}
    	
    	List<PvUvArgs> res = new ArrayList<PvUvArgs>();
		res.add(new PvUvArgs(key, uid, EditorEvaluationKeyBuilder.getColumnName(StatisticsIndicator.genTie, platform, ShareBackChannel_EE.all), 1l));
		res.add(new PvUvArgs(key, uid, EditorEvaluationKeyBuilder.getColumnName(StatisticsIndicator.genTie, Platform.all, ShareBackChannel_EE.all), 1l));
		
		return res;
	}

}
