package com.netease.weblogOffline.hive;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.netease.weblogCommon.data.enums.NeteaseChannel_CS;
import com.netease.weblogCommon.logparsers.LogParser;
import com.netease.weblogCommon.logparsers.ZyLogParams;
import com.netease.weblogCommon.logparsers.ZylogParser;
import com.netease.weblogCommon.utils.TextUtils;
import com.netease.weblogCommon.utils.UrlUtils;
import com.netease.weblogOffline.common.zylogfilter.ZylogFilterUtils;


/**
 * 章鱼日志解析
 */
@SuppressWarnings("deprecation")
public class ZyLogSerde implements Deserializer {

    private static List<String> structFieldNames = new ArrayList<String>();
    private static List<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();
    private static LogParser logParser = new ZylogParser();



    static {
        structFieldNames.add(ZyLogParams.logTime);
        structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        structFieldNames.add(ZyLogParams.logTimeFormat);
        structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        structFieldNames.add(ZyLogParams.uid);
        structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        structFieldNames.add(ZyLogParams.nvtm);
        structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        structFieldNames.add(ZyLogParams.nvfi);
        structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        structFieldNames.add(ZyLogParams.nvsf);
        structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        structFieldNames.add(ZyLogParams.loginStatus);
        structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        structFieldNames.add(ZyLogParams.url);
        structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        structFieldNames.add("pureUrl");
        structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        structFieldNames.add("contentChannel");
        structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        structFieldNames.add(ZyLogParams.title);
        structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        structFieldNames.add(ZyLogParams.reference);
        structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        structFieldNames.add(ZyLogParams.userAgent);
        structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        structFieldNames.add(ZyLogParams.resolution);
        structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        structFieldNames.add(ZyLogParams.langurage);
        structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        structFieldNames.add(ZyLogParams.screenColorDepth);
        structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        structFieldNames.add(ZyLogParams.lastModifyTime);
        structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        structFieldNames.add(ZyLogParams.ip);
        structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        structFieldNames.add(ZyLogParams.province);
        structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        structFieldNames.add(ZyLogParams.city);
        structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        structFieldNames.add(ZyLogParams.supporter);
        structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        structFieldNames.add(ZyLogParams.email);
        structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        structFieldNames.add(ZyLogParams.flashVersion);
        structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
    }

    public Object deserialize(Writable writable) throws SerDeException {
        List<Object> result = new ArrayList<Object>();
        try {
            Text rowText = (Text) writable;
            Map<String,String> linemap = logParser.parse(rowText.toString());
            if(linemap!=null && linemap.size()>0){
                result.add(TextUtils.notNullStr(linemap.get(ZyLogParams.logTime),ZylogFilterUtils.defNullStr));
                result.add(TextUtils.notNullStr(linemap.get(ZyLogParams.logTimeFormat),ZylogFilterUtils.defNullStr));
                result.add(TextUtils.notNullStr(linemap.get(ZyLogParams.uid),ZylogFilterUtils.defNullStr));
                result.add(TextUtils.notNullStr(linemap.get(ZyLogParams.nvtm),ZylogFilterUtils.defNullStr));
                result.add(TextUtils.notNullStr(linemap.get(ZyLogParams.nvfi),ZylogFilterUtils.defNullStr));
                result.add(TextUtils.notNullStr(linemap.get(ZyLogParams.nvsf),ZylogFilterUtils.defNullStr));
                result.add(TextUtils.notNullStr(linemap.get(ZyLogParams.loginStatus),ZylogFilterUtils.defNullStr));
                result.add(TextUtils.notNullStr(linemap.get(ZyLogParams.url),ZylogFilterUtils.defNullStr));
                String pureUrl = UrlUtils.getOriginalUrl(linemap.get(ZyLogParams.url));
                result.add(TextUtils.notNullStr(pureUrl,ZylogFilterUtils.defNullStr));
                result.add(TextUtils.notNullStr(NeteaseChannel_CS.getChannelName(pureUrl),ZylogFilterUtils.defNullStr));
                result.add(TextUtils.notNullStr(linemap.get(ZyLogParams.title),ZylogFilterUtils.defNullStr));
                result.add(TextUtils.notNullStr(linemap.get(ZyLogParams.reference),ZylogFilterUtils.defNullStr));
                result.add(TextUtils.notNullStr(linemap.get(ZyLogParams.userAgent),ZylogFilterUtils.defNullStr));
                result.add(TextUtils.notNullStr(linemap.get(ZyLogParams.resolution),ZylogFilterUtils.defNullStr));
                result.add(TextUtils.notNullStr(linemap.get(ZyLogParams.langurage),ZylogFilterUtils.defNullStr));
                result.add(TextUtils.notNullStr(linemap.get(ZyLogParams.screenColorDepth),ZylogFilterUtils.defNullStr));
                result.add(TextUtils.notNullStr(linemap.get(ZyLogParams.lastModifyTime),ZylogFilterUtils.defNullStr));
                result.add(TextUtils.notNullStr(linemap.get(ZyLogParams.ip),ZylogFilterUtils.defNullStr));
            	result.add(TextUtils.notNullStr(linemap.get(ZyLogParams.province),ZylogFilterUtils.defNullStr));
                result.add(TextUtils.notNullStr(linemap.get(ZyLogParams.city),ZylogFilterUtils.defNullStr));
                result.add(TextUtils.notNullStr(linemap.get(ZyLogParams.supporter),ZylogFilterUtils.defNullStr));
                result.add(TextUtils.notNullStr(linemap.get(ZyLogParams.email),ZylogFilterUtils.defNullStr));
                result.add(TextUtils.notNullStr(linemap.get(ZyLogParams.flashVersion),ZylogFilterUtils.defNullStr));
            }


        } catch (Exception localException1) {}

        return result;
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
