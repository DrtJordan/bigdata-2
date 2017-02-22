package com.netease.weblogOffline.statistics.editorEvaluation.hbase;

import java.net.MalformedURLException;

import org.apache.solr.client.solrj.impl.CloudSolrServer;

public class SolrUtil {
    
    private static final String zkHost = "123.58.179.51:2181,123.58.179.52:2181,123.58.179.53:2181/solr";
    private static final String defaultCollection = "collection6";
    private static final int zkClientTimeout = 20000;
    private static final int zkConnectTimeout = 1000;
    
    public static CloudSolrServer getCloudSolrServer() {
        CloudSolrServer cloudSolrServer = null;
        try {
            cloudSolrServer = new CloudSolrServer(zkHost);
            cloudSolrServer.setDefaultCollection(defaultCollection);
            cloudSolrServer.setZkClientTimeout(zkClientTimeout);
            cloudSolrServer.setZkConnectTimeout(zkConnectTimeout);
            cloudSolrServer.connect();
            System.out.println("The cloud Server has been connected !!!!");
        } catch (MalformedURLException e) {
            System.out.println("The URL of zkHost is not correct!! Its form must as below:\n zkHost:port");
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return cloudSolrServer;
    }
 
    
}
