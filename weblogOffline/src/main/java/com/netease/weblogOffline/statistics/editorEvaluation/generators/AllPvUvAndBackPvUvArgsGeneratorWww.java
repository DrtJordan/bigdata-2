package com.netease.weblogOffline.statistics.editorEvaluation.generators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.netease.weblogCommon.data.enums.Platform;
import com.netease.weblogCommon.data.enums.ShareBackChannel_CS;
import com.netease.weblogCommon.data.enums.ShareBackChannel_EE;
import com.netease.weblogCommon.data.enums.StatisticsIndicator;
import com.netease.weblogCommon.tools.EditorEvaluationKeyBuilder;
import com.netease.weblogCommon.utils.UrlUtils;
import com.netease.weblogOffline.statistics.editorEvaluation.data.PvUvArgs;
import com.netease.weblogOffline.statistics.editorEvaluation.needColumn.NeedColumnUtils;

public class AllPvUvAndBackPvUvArgsGeneratorWww implements PvUvArgsGenerator{

	@Override
	public List<PvUvArgs> execute(String line) {
		ArrayList<PvUvArgs> al = new ArrayList<PvUvArgs>();
		HashMap<String, String> hm =NeedColumnUtils.buildKVMapOfZyOnput(line);
		

		String pureUrl = UrlUtils.getOriginalUrl(hm.get("url"));
		pureUrl = UrlUtils.mergeAticleMultiPage(pureUrl);
		PvUvArgs cell1 = new PvUvArgs(pureUrl, hm.get("uid"),
				EditorEvaluationKeyBuilder.getColumnName(StatisticsIndicator.base, Platform.www, ShareBackChannel_EE.all), 
				1l);
	
		al.add(cell1);
		String suffix = null;
		if (null != (suffix = ShareBackChannel_CS.getBackChannel(hm.get("url")))){
			PvUvArgs cell2 = new PvUvArgs(pureUrl, hm.get("uid"), 
					EditorEvaluationKeyBuilder.getColumnName(StatisticsIndicator.back, Platform.www, ShareBackChannel_EE.all),
					1l);
			PvUvArgs cell3 = new PvUvArgs(pureUrl, hm.get("uid"), 
					EditorEvaluationKeyBuilder.getColumnName(StatisticsIndicator.back, Platform.www, ShareBackChannel_EE.getBySign_3w(suffix)),
					1l);			
			al.add(cell2);
			al.add(cell3);
		 }
		
		return al;

	}

}
