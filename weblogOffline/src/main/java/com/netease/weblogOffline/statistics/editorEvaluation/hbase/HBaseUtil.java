package com.netease.weblogOffline.statistics.editorEvaluation.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.security.UserGroupInformation;

public class HBaseUtil {
    
    private static Configuration conf = null;

    /**
     * 初始化配置
     */
    static {
        // kerberos的配置文件的位置,windows下叫krb5.ini,linux下叫krb5.conf
        System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
        // 会自动加载hbase-site.xml
        conf = HBaseConfiguration.create();

        // 使用keytab登陆
        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation.loginUserFromKeytab("weblog/dev@HADOOP.HZ.NETEASE.COM", "/home/weblog/weblog.keytab");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static Configuration getConfiguration() {
        return conf;
    }
    
    
}
