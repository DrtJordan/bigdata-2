package com.netease.weblogOffline.statistics.editorEvaluation.test;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkStateReader;

public class CloudSolrServerTest {
  private static CloudSolrServer cloudSolrServer;

//    private static final SolrServer solrServer2 = new ConcurrentUpdateSolrServer(
//        solrUrl, 10000, 20);
  private static synchronized CloudSolrServer getCloudSolrServer(final String zkHost) {
    if (cloudSolrServer == null) {
      try {
        cloudSolrServer = new CloudSolrServer(zkHost);
      } catch (MalformedURLException e) {
        System.out.println("The URL of zkHost is not correct!! Its form must as below:\n zkHost:port");
        e.printStackTrace();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return cloudSolrServer;
  }

  private void addIndex(SolrServer solrServer) {
    try {
      SolrInputDocument doc1 = new SolrInputDocument();
      doc1.addField("id", "1");
      doc1.addField("name", "张民");
      SolrInputDocument doc2 = new SolrInputDocument();
      doc2.addField("id", "2");
      doc2.addField("name", "刘俊");
      SolrInputDocument doc3 = new SolrInputDocument();
      doc3.addField("id", "3");
      doc3.addField("name", "刘俊2");
      Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
      docs.add(doc1);
      docs.add(doc2);
      docs.add(doc3);
      solrServer.add(docs);
      solrServer.commit();
    } catch (SolrServerException e) {
      System.out.println("Add docs Exception !!!");
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (Exception e) {
      System.out.println("Unknowned Exception!!!!!");
      e.printStackTrace();
    }
  }

  private void addRandomIndex(SolrServer solrServer) {
    int i = 0;
    while (i < 10000) {
      try {
        for (int j = 0; j < 100; j++) {
          SolrInputDocument doc1 = new SolrInputDocument();
          doc1.addField("id", String.valueOf(i));
          doc1.addField("name", "张民");
          Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
          docs.add(doc1);
          solrServer.add(docs);
          i++;
        }
        solrServer.commit();
      } catch (SolrServerException e) {
        System.out.println("Add docs Exception !!!");
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      } catch (Exception e) {
        System.out.println("Unknowned Exception!!!!!");
        e.printStackTrace();
      }
    }

  }

  public void search(SolrServer solrServer, String String) {
    SolrQuery query = new SolrQuery();
    query.setQuery(String);
    try {
      QueryResponse response = solrServer.query(query);
      SolrDocumentList docs = response.getResults();
      System.out.println("文档个数：" + docs.getNumFound());
      System.out.println("查询时间：" + response.getQTime());
      for (SolrDocument doc : docs) {
        String name = (String) doc.getFieldValue("name");
        String id = (String) doc.getFieldValue("id");
        System.out.println("id: " + id);
        System.out.println("name: " + name);
        System.out.println();
      }
    } catch (SolrServerException e) {
      e.printStackTrace();
    } catch (Exception e) {
      System.out.println("Unknowned Exception!!!!");
      e.printStackTrace();
    }
  }

  public void deleteAllIndex(SolrServer solrServer) {
    try {
      solrServer.deleteByQuery("*:*");// delete everything!
      solrServer.commit();
    } catch (SolrServerException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (Exception e) {
      System.out.println("Unknowned Exception !!!!");
      e.printStackTrace();
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    final String zkHost = "123.58.179.51:2181,123.58.179.52:2181,123.58.179.53:2181/solr";
    final String defaultCollection = "collection1";
    final int zkClientTimeout = 20000;
    final int zkConnectTimeout = 1000;
    CloudSolrServer cloudSolrServer = getCloudSolrServer(zkHost);
    System.out.println("The Cloud SolrServer Instance has benn created!");
    cloudSolrServer.setDefaultCollection(defaultCollection);
    cloudSolrServer.setZkClientTimeout(zkClientTimeout);
    cloudSolrServer.setZkConnectTimeout(zkConnectTimeout);
    cloudSolrServer.connect();
    System.out.println("The cloud Server has been connected !!!!");
    ZkStateReader zkStateReader = cloudSolrServer.getZkStateReader();
    ClusterState cloudState = zkStateReader.getClusterState();
    System.out.println(cloudState);
    // 测试实例！
    CloudSolrServerTest test = new CloudSolrServerTest();
    // System.out.println("测试添加index！！！");
     test.addIndex(cloudSolrServer);
    // System.out.println("测试查询query！！！！");
     test.search(cloudSolrServer, "id:*");
    // System.out.println("测试删除！！！！");
    // test.deleteAllIndex(cloudSolrServer);
    // System.out.println("删除所有文档后的查询结果：");
    // test.search(cloudSolrServer, "*:*");
    long s1 = System.currentTimeMillis();
    test.addRandomIndex(cloudSolrServer);
    long s2 = System.currentTimeMillis();
    System.out.println(s2 - s1);


    test.search(cloudSolrServer, "id:9999");


    // 查询语法
    SolrQuery q = new SolrQuery();
    // 基本的字段查询
    q.setQuery("TITLE:中国人");
    // 多字段或关系 TITLE:("中国人" AND "美国人" AND "英国人")
    // 多字段不包含的关系 TITLE:(* NOT "上网费用高" NOT "宽带收费不合理" )
    // 查询一个范围 BETWEEN 适用于数字和日期类型 NUM:[-90 TO 360 ] OR CREATED_AT:[" + date1 + " TO " + date2 + "]
    // 日期转换 不是惯用的 yyyy-MM-dd HH:mm:ss
    //String date1 = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").format(c.getStartTime().getTime());


      // release the resource
    cloudSolrServer.shutdown();
  }
}
