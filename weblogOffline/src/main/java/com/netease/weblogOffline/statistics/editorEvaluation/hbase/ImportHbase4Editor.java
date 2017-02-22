package com.netease.weblogOffline.statistics.editorEvaluation.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import com.netease.weblogCommon.data.enums.ContentAttributions;
import com.netease.weblogCommon.tools.EditorEvaluationKeyBuilder;
import com.netease.weblogCommon.utils.BytesUtils;
import com.netease.weblogOffline.data.MultiStatisticResultWrapWritable;
import com.netease.weblogOffline.data.StatisticResultWritable;

public class ImportHbase4Editor extends DealHDFSThread {

    public static final String TABLE_NAME = "datacube:editor"; // 录入目的表名
    public static final String FAMILY = "values"; // 列簇
    public static final String QUALIFIER_CONF = "conf"; // conf列
    public static final String QUALIFIER_PVUV = "pvuv"; // uv列
    public static int BATCH_PUT_NUM = 10000;

    private static Configuration conf = HBaseUtil.getConfiguration();
    
    /**
     * rowKey分隔符
     */
    private static final String splitStr = "|=|";

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("args error!");
            return;
        }

        String resDir = args[0];
        String dt = args[1];

        try {
            new ImportHbase4Editor().execute(resDir, dt);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private String getRowKey(String key, String dt) {
        int index = key.indexOf(",");
        String editor = key.substring(0, index);
        String platform = key.substring(index + 1);
        String rowKey = editor + splitStr + platform;
        String hashCode = rowKey.hashCode() + "";
        if (hashCode.startsWith("-")) {
            hashCode = hashCode.substring(1);
        }
        return hashCode + splitStr + rowKey + splitStr + dt;
    }

    @Override
    protected void build(Path path, String dt, Map<String, Long> countMap) {
        String fileName = path.getName();
        
        int count = 0;
        Map<String, Integer> exMap = new HashMap<String, Integer>();
        Set<String> exGroupKey = new HashSet<String>();

        HTable table = null;
        SequenceFile.Reader reader = null;
        try {
            table = new HTable(conf, TABLE_NAME);
            ArrayList<Put> list = new ArrayList<Put>();

            FileSystem fs = FileSystem.get(new Configuration());
            reader = new SequenceFile.Reader(fs, path, conf);

            Text key = new Text();
            MultiStatisticResultWrapWritable value = new MultiStatisticResultWrapWritable();

            Map<Integer, String> pvuvMap = new HashMap<>();
            Map<String, String> confMap = new HashMap<>();
            
            String exKey = null;
            Integer exCount = null;
            
            while (reader.next(key, value)) {
                try {
                    count++;

                    if (list.size() > BATCH_PUT_NUM) {
                        table.put(list);
                        table.flushCommits();
                        list.clear();
                    }

                    pvuvMap.clear();
                    confMap.clear();
                    
                    // toHbase
                    String rowKey = getRowKey(key.toString(), dt);// 有key组成
                    Put p = new Put(rowKey.getBytes());

                    Map<Text, StatisticResultWritable> dataMap = value.getMsr().getDataMap();
                    for (Entry<Text, StatisticResultWritable> entry : dataMap.entrySet()) {
                        String k = entry.getKey().toString();
                        Integer pvuvkey = EditorEvaluationKeyBuilder.compactColumnName(k);
                        if (pvuvkey == null) {
                            exGroupKey.add(k);
                            continue;
                        }
                        pvuvMap.put(pvuvkey, entry.getValue().getPv() + "," + entry.getValue().getUv());
                    }
                    
                    for(Entry<String, String> entry : value.getConf().entrySet()){
                        String mkey = entry.getKey();
                        if (ContentAttributions.editor_3w.getName().equals(mkey)
                                || ContentAttributions.editor_3g.getName().equals(mkey)) {
                            confMap.put(mkey, entry.getValue());
                        }
                    }
                    
                    p.add(FAMILY.getBytes(), QUALIFIER_CONF.getBytes(), BytesUtils.ssToBytes(confMap));
                    p.add(FAMILY.getBytes(), QUALIFIER_PVUV.getBytes(), BytesUtils.isToBytes(pvuvMap));
                    list.add(p);

                } catch (Exception e) {
                    exKey = e.getMessage();
                    exCount = exMap.get(exKey);
                    if(exCount == null){
                        exCount = 0;
                        e.printStackTrace();
                    }
                    exMap.put(exKey, exCount + 1);
                }

            }

            if (list.size() > 0) {
                table.put(list);
                table.flushCommits();
            }
            
            if (!exGroupKey.isEmpty()) {
                StringBuffer exKeys = new StringBuffer();
                for (String k : exGroupKey) {
                    exKeys.append(k).append("; ");
                }
                System.out.println(fileName + " exception groupKeys : " + exKeys.toString());
            }
            
            if (!exMap.isEmpty()) {
                System.out.println(fileName + " exceptions : \n" + exMap.toString());
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        countMap.put(fileName, (long) count); //线程计数
    }

}
