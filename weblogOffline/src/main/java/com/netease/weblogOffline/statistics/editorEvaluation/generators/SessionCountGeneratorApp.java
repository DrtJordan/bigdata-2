package com.netease.weblogOffline.statistics.editorEvaluation.generators;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.netease.weblogCommon.data.enums.Platform;
import com.netease.weblogCommon.data.enums.ShareBackChannel_EE;
import com.netease.weblogCommon.data.enums.StatisticsIndicator;
import com.netease.weblogCommon.tools.EditorEvaluationKeyBuilder;
import com.netease.weblogOffline.statistics.editorEvaluation.data.PvUvArgs;
import com.netease.weblogCommon.utils.JsonUtils;

/**
 * ymshang 20151109
 * 移动format日志解析。
 * /ntes_weblog/weblog/statistics/editorEvaluation/mobileFormatLog/
 **/
public class SessionCountGeneratorApp implements PvUvArgsGenerator{

	@Override
	public List<PvUvArgs> execute(String line) {
		// TODO Auto-generated method stub
		ArrayList<PvUvArgs> al = new ArrayList<PvUvArgs>();

		Map<String, String> logMap = JsonUtils.json2StrMap(line);
		if("_pvX".equals(logMap.get("eventname"))||"_ivX".equals(logMap.get("eventname"))||"_vvX".equals(logMap.get("eventname"))||"_svX".equals(logMap.get("eventname"))){
			PvUvArgs cell1 = new PvUvArgs(logMap.get("eventtag"), logMap.get("sessionid"),
					EditorEvaluationKeyBuilder.getColumnName(StatisticsIndicator.sessionCount, Platform.app, ShareBackChannel_EE.all),
					Long.parseLong(logMap.get("acc")));
			al.add(cell1);
		}
		return al;
	}

}
