package com.netease.weblogOffline.statistics.editorEvaluation.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.security.UserGroupInformation;

public class HbaseTest {
	private static Configuration conf = null;
	public static void main(String[] args) {
		try {
			System.out.println("begin");
			Configuration HBASE_CONFIG = new Configuration();
			HBASE_CONFIG.set("hbase.zookeeper.quorum", "hbase0.photo.163.org,hbase1.photo.163.org,hbase2.photo.163.org");
			HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", "2181");
			conf = HBaseConfiguration.create(HBASE_CONFIG);

			System.setProperty("java.security.krb5.conf", "C:/Windows/krb5.ini");
			UserGroupInformation.setConfiguration(conf);

			UserGroupInformation.loginUserFromKeytab("weblog/dev@HADOOP.HZ.NETEASE.COM", "E:/Job/Downloads/weblog.keytab");
			
			getOneRecord("datacube:testdchtuserremainiphone", "appstore20150608");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void getOneRecord(String tableName, String rowKey)
			throws IOException {
		HTable table = new HTable(conf, tableName);
		Get get = new Get(rowKey.getBytes());
		Result rs = table.get(get);
		for (KeyValue kv : rs.raw()) {
			System.out.print(new String(kv.getRow()) + " ");
			System.out.print(new String(kv.getFamily()) + ":");
			System.out.print(new String(kv.getQualifier()) + " ");
			System.out.print(kv.getTimestamp() + " ");
			System.out.println(new String(kv.getValue()));
		}
	}

}
