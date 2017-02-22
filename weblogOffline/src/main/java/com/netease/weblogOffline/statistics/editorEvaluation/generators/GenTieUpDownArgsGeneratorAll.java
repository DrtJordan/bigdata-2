package com.netease.weblogOffline.statistics.editorEvaluation.generators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.netease.weblogCommon.data.enums.Platform;
import com.netease.weblogCommon.data.enums.ShareBackChannel_EE;
import com.netease.weblogCommon.data.enums.StatisticsIndicator;
import com.netease.weblogCommon.tools.EditorEvaluationKeyBuilder;
import com.netease.weblogOffline.statistics.editorEvaluation.data.PvUvArgs;
import com.netease.weblogOffline.statistics.editorEvaluation.needColumn.NeedColumnUtils;

public class GenTieUpDownArgsGeneratorAll implements PvUvArgsGenerator {

	@Override
	public List<PvUvArgs> execute(String line) {
		ArrayList<PvUvArgs> al = new ArrayList<PvUvArgs>();
		HashMap<String, String> hm = NeedColumnUtils.buildKVMapOfUpDown(line);
		long m = Long.parseLong(hm.get("count"));
		if (m >= 0) {
			PvUvArgs cell = new PvUvArgs(hm.get("danTieID"),
					hm.get("userId"),
					EditorEvaluationKeyBuilder.getColumnName(StatisticsIndicator.genTieUp, Platform.all, ShareBackChannel_EE.all),
					m);
			al.add(cell);
		} else {
			PvUvArgs cell = new PvUvArgs(hm.get("danTieID"),
					hm.get("userId"),
					EditorEvaluationKeyBuilder.getColumnName(StatisticsIndicator.genTieDown, Platform.all, ShareBackChannel_EE.all),
					Math.abs(m));
			al.add(cell);
		}
		return al;

	}

}
