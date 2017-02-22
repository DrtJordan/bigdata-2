package com.netease.weblogOffline.statistics.editorEvaluation.generators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.netease.weblogCommon.data.enums.Platform;
import com.netease.weblogCommon.data.enums.ShareBackChannel_EE;
import com.netease.weblogCommon.data.enums.StatisticsIndicator;
import com.netease.weblogCommon.tools.EditorEvaluationKeyBuilder;
import com.netease.weblogCommon.utils.UrlUtils;
import com.netease.weblogOffline.statistics.editorEvaluation.data.PvUvArgs;
import com.netease.weblogOffline.statistics.editorEvaluation.needColumn.NeedColumnUtils;

public class PvUvArgsGeneratorWap implements PvUvArgsGenerator {

    @Override
    public List<PvUvArgs> execute(String line) throws Exception {
        ArrayList<PvUvArgs> list = new ArrayList<PvUvArgs>();

        HashMap<String, String> hm =NeedColumnUtils.buildKVMapOfZyOnput(line);
        
        String pureUrl = UrlUtils.getOriginalUrl(hm.get("url"));
        pureUrl = UrlUtils.mergeAticleMultiPage(pureUrl);
        PvUvArgs cell = new PvUvArgs(pureUrl, hm.get("uid"),
        		EditorEvaluationKeyBuilder.getColumnName(StatisticsIndicator.base, Platform.wap, ShareBackChannel_EE.all),
                1l);
        list.add(cell);

        return list;
    }

}
