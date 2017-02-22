package com.netease.weblogOffline.statistics.editorEvaluation.hbase;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.solr.client.solrj.impl.CloudSolrServer;

import com.netease.datacube.bean.evaluation.SolrEvaluation;
import com.netease.weblogCommon.data.enums.ContentAttributions;
import com.netease.weblogCommon.data.enums.SimpleDateFormatEnum;
import com.netease.weblogCommon.utils.DateUtils;
import com.netease.weblogCommon.utils.JsonUtils;
import com.netease.weblogOffline.data.HashMapStringStringWritable;

public class BuildIndex extends DealHDFSThread{

    public static int BATCH_PUT_NUM = 1000;

    private static final String splitStr = "|=|";

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("args error!");
            return;
        }

        String resDir = args[0];
        String dt = args[1];

        try {
            new BuildIndex().execute(resDir, dt);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private String getRowKey(String url, HashMap<String, String> hm) {
        String id = hm.get(ContentAttributions.id_3w.getName());
        if (StringUtils.isBlank(id)) {
            id = "";
        }
        String rowKey = id + splitStr + url;
        String hashCode = rowKey.hashCode() + "";
        if (hashCode.startsWith("-")) {
            hashCode = hashCode.substring(1);
        }
        return hashCode + splitStr + rowKey;
    }

    @Override
    protected void build(Path path, String dt, Map<String, Long> countMap) {
        String fileName = path.getName();
        CloudSolrServer solrServer = SolrUtil.getCloudSolrServer();
        
        int count = 0;
        int errorCount = 0;
        Map<String, Integer> exMap = new HashMap<String, Integer>();
        
        SequenceFile.Reader reader = null;
        
        try {
            List<SolrEvaluation> list = new ArrayList<SolrEvaluation>();
            
            FileSystem fs = FileSystem.get(new Configuration());
            reader = new SequenceFile.Reader(fs, path, new Configuration());

            Text key = new Text();
            HashMapStringStringWritable value = new HashMapStringStringWritable();
            String exKey = null;
            Integer exCount = null;
            while (reader.next(key, value)) {
                try {
                    count++;

                    if (list.size() > BATCH_PUT_NUM) {
                        solrServer.addBeans(list);
                        solrServer.commit();
                        System.out.println(fileName + ": dealt count : " + (count - 1));
                        list.clear();
                    }

                    SolrEvaluation bean = new SolrEvaluation();
                    HashMap<String, String> hm = value.getHm();
                    String rowKey = getRowKey(key.toString(), hm);
                    fillSolrBean(bean, hm, rowKey);
                    if (bean.getCtime() == null || bean.getEtime() == null) {
                        errorCount++;
                        continue;
                    }
                    list.add(bean);

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
                solrServer.addBeans(list);
                solrServer.commit();
                System.out.println(fileName + ": dealt count : " + count);
            }

            if (errorCount > 0) {
                System.out.println(fileName + " no ctime or etime count : " + errorCount);
            }
            
            if (!exMap.isEmpty()) {
                System.out.println(fileName + " exceptions : \n" + exMap.toString());
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            if(solrServer != null){
                solrServer.shutdown();
            }
        }

        countMap.put(fileName, (long) count); // 线程计数
    }

    /**
     * @param bean
     * @param hm
     * @param rowKey
     * @throws Exception 
     */
    private void fillSolrBean(SolrEvaluation bean, HashMap<String, String> hm, String rowKey) throws Exception {
        bean.setKey(rowKey);
        bean.setId(hm.get(ContentAttributions.id_3w.getName()));
        bean.setUrl_3w(hm.get(ContentAttributions.url_3w.getName()));
        bean.setEditor_3w(hm.get(ContentAttributions.editor_3w.getName()));
        bean.setEditor_3G(hm.get(ContentAttributions.editor_3g.getName()));
        
        SimpleDateFormat dateFormat = SimpleDateFormatEnum.dateFormat.get();
        SimpleDateFormat zyLogTimeFormat = SimpleDateFormatEnum.zyLogTimeFormat.get();
        String publishTime_3w = hm.get(ContentAttributions.publishTime_3w.getName());
        String activeFlag = hm.get(ContentAttributions.activeFlag.getName());
        if (StringUtils.isNotBlank(publishTime_3w)) {
            String publishDate = DateUtils.dateFormatTransform(publishTime_3w, zyLogTimeFormat, dateFormat);
            bean.setCtime(dateFormat.parse(publishDate).getTime());
            if (activeFlag == null) {
                activeFlag = "";
            }
            bean.setDateindex(activeFlag);
            String lastActiveDt = ContentAttributions.ActiveFlagUtils.getLastActiveDt(activeFlag, publishDate);
            bean.setEtime(dateFormat.parse(lastActiveDt).getTime());
        }

        Map<String, String> confMap = new HashMap<String, String>();
        if(hm.get(ContentAttributions.id_3g.getName()) != null){
            confMap.put(ContentAttributions.id_3g.getName(), hm.get(ContentAttributions.id_3g.getName()));
        }
        if(hm.get(ContentAttributions.url_3g.getName()) != null){
            confMap.put(ContentAttributions.url_3g.getName(), hm.get(ContentAttributions.url_3g.getName()));
        }
        if(hm.get(ContentAttributions.source.getName()) != null){
            confMap.put(ContentAttributions.source.getName(), hm.get(ContentAttributions.source.getName()));
        }
        if(hm.get(ContentAttributions.author.getName()) != null){
            confMap.put(ContentAttributions.author.getName(), hm.get(ContentAttributions.author.getName()));
        }
        if(hm.get(ContentAttributions.isOriginal.getName()) != null){
            confMap.put(ContentAttributions.isOriginal.getName(), hm.get(ContentAttributions.isOriginal.getName()));
        }
        if(hm.get(ContentAttributions.docid_3g.getName()) != null){
            confMap.put(ContentAttributions.docid_3g.getName(), hm.get(ContentAttributions.docid_3g.getName()));
        }
        if(hm.get(ContentAttributions.title_3w.getName()) != null){
            confMap.put(ContentAttributions.title_3w.getName(), hm.get(ContentAttributions.title_3w.getName()));
        }
        if(hm.get(ContentAttributions.title_3g.getName()) != null){
            confMap.put(ContentAttributions.title_3g.getName(), hm.get(ContentAttributions.title_3g.getName()));
        }
        if(hm.get(ContentAttributions.topic_3w.getName()) != null){
            confMap.put(ContentAttributions.topic_3w.getName(), hm.get(ContentAttributions.topic_3w.getName()));
        }
        if(hm.get(ContentAttributions.topic_3g.getName()) != null){
            confMap.put(ContentAttributions.topic_3g.getName(), hm.get(ContentAttributions.topic_3g.getName()));
        }
        if(hm.get(ContentAttributions.type_3w.getName()) != null){
            confMap.put(ContentAttributions.type_3w.getName(), hm.get(ContentAttributions.type_3w.getName()));
        }
        if(hm.get(ContentAttributions.type_3g.getName()) != null){
            confMap.put(ContentAttributions.type_3g.getName(), hm.get(ContentAttributions.type_3g.getName()));
        }
        if(hm.get(ContentAttributions.channel_3w.getName()) != null){
            confMap.put(ContentAttributions.channel_3w.getName(), hm.get(ContentAttributions.channel_3w.getName()));
        }
        if(hm.get(ContentAttributions.channel_3g.getName()) != null){
            confMap.put(ContentAttributions.channel_3g.getName(), hm.get(ContentAttributions.channel_3g.getName()));
        }
        if(hm.get(ContentAttributions.publishTime_3w.getName()) != null){
            confMap.put(ContentAttributions.publishTime_3w.getName(), hm.get(ContentAttributions.publishTime_3w.getName()));
        }
        if(hm.get(ContentAttributions.publishTime_3g.getName()) != null){
            confMap.put(ContentAttributions.publishTime_3g.getName(), hm.get(ContentAttributions.publishTime_3g.getName()));
        }
        bean.setConf(JsonUtils.toJson(confMap));

    }

}
