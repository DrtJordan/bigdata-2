package com.netease.weblogOffline.statistics.editorEvaluation.hbase;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;



public class ElasticUtil {

    public static TransportClient getEsClient() {
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "hbase_index_test").build();
        TransportClient client = new TransportClient(settings);
        try {
            client.addTransportAddress(new InetSocketTransportAddress("10.164.99.149", 9300))
                    .addTransportAddress(new InetSocketTransportAddress("10.164.99.150", 9300));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return client;
}
    
}
