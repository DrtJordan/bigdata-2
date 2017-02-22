package com.netease.weblogOffline.common;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.log4j.Logger;

import com.netease.weblogCommon.utils.HttpClientFactory;

public class DataCubeTool {
    
    private static Logger LOG = Logger.getLogger(DataCubeTool.class);
    
    public static void sendToDC(String date, String content, String dcHost){
        
        LOG.info("date: " + date);
        LOG.info("content: " + content);
        LOG.info("dcHost=" + dcHost);
        
        if(null == dcHost){
            LOG.error("post status: null dchost");
            LOG.info(date + " " + content);
        }else{
            HttpClient httpClient = HttpClientFactory.getNewInstance();
            
            PostMethod pm = HttpClientFactory.postMethod(dcHost);
            
            pm.addParameter("day", date);
            pm.addParameter("data", content);

            try {
                int status = httpClient.executeMethod(pm);
                LOG.info("post status: " + status);
            } catch (Exception e) {
                LOG.error("post status: error");
            }
        }
    }
}
