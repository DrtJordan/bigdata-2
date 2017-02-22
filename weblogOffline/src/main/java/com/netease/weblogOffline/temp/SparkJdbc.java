package com.netease.weblogOffline.temp;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.security.UserGroupInformation;
public class SparkJdbc {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		 org.apache.hadoop.conf.Configuration conf = new     
				org.apache.hadoop.conf.Configuration();
		 
		        System.setProperty("hadoop.home.dir", "d:\\");
			    conf.set("hadoop.security.authentication", "Kerberos");
				UserGroupInformation.setConfiguration(conf);
		    UserGroupInformation.loginUserFromKeytab("weblog/dev@HADOOP.HZ.NETEASE.COM", "C:\\Users\\liyonghong\\Downloads\\weblog.keytab");
	        String driver = "org.apache.hive.jdbc.HiveDriver";
	        String dbName = "weblog";
	        String passwrod = "123456";
	        String userName = "weblog";
	        String url = "jdbc:hive2://123.58.179.98:10000/"+dbName+";principal=weblog/dev@HADOOP.HZ.NETEASE.COM";
	        String sql1 = "use weblog";
	        String sql2 = "add jar /home/weblog/weblogOffline/webAnalysis/weblogOffline-0.1.0-jar-with-dependencies.jar";
	        String sql3 = "set mapred.job.queue.name=weblog";
	      //  String url = "jdbc:hive2://123.58.179.98:10000/" + dbName;
	        String sql = "select * from weblog  limit 10";
	      // String sql = "show tables";
	       // String addjar = "add jar /home/weblog/weblogOffline/webAnalysis/weblogOffline-0.1.0-jar-with-dependencies.jar";
	        //+";principal=weblog/dev@HADOOP.HZ.NETEASE.COM"
	        try {
	       

	            Class.forName(driver);

	            Connection conn = DriverManager.getConnection(url, userName,

	                    passwrod);
	            PreparedStatement ps1 = conn.prepareStatement(sql1);
	            PreparedStatement ps2 = conn.prepareStatement(sql2);
	            PreparedStatement ps3 = conn.prepareStatement(sql3);
	            PreparedStatement ps = conn.prepareStatement(sql);
	            
	           
	                ps1.executeQuery();
	                ps2.executeQuery();
	                ps3.executeQuery();

	            ResultSet rs = ps.executeQuery();

	            while (rs.next()) {
//ip        |   servertime   |           project           |   event    | esource  | einfo  |   cost   |       uuid        |                                   url 
	                System.out.println("ip : " + rs.getString(1)+ " servertime : "

	                        + rs.getString(2) + " project : " + rs.getString(3) + " event : "

	                        + rs.getString(4)  + rs.getString(1)+ " esource : "

	                        + rs.getString(5)  + rs.getString(1)+ " einfo : "

	                        + rs.getString(6)  + rs.getString(1)+ " cost : "

	                        + rs.getString(7)  + rs.getString(1)+ " uuid : "

	                        + rs.getString(8)  + rs.getString(1)+ " url : "

	                        + rs.getString(9) );

	            }

	            // 关闭记录集

	            if (rs != null) {

	                try {

	                    rs.close();

	                } catch (SQLException e) {

	                    e.printStackTrace();

	                }

	            }
	            // 关闭声明

	            if (ps != null) {

	                try {

	                    ps.close();

	                } catch (SQLException e) {

	                    e.printStackTrace();

	                }

	            }

	            // 关闭链接对象

	            if (conn != null) {

	                try {

	                    conn.close();

	                } catch (SQLException e) {

	                    e.printStackTrace();

	                }

	            }

	        } catch (Exception e) {

	            e.printStackTrace();

	        }
	}

}
