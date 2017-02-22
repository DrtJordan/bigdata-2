/**
 * @(#)ConfigUtils.java, 2012-11-29. 
 * 
 * Copyright 2012 Netease, Inc. All rights reserved.
 * NETEASE PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.netease.weblogOffline.utils;


import java.io.File;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;

public class ConfigUtils {

    private static Logger LOG = Logger.getLogger(ConfigUtils.class);
    

    /**
     * Parse the property configuration file.
     * 
     * @param confDir
     * @param fileName
     * @return
     * @throws org.apache.commons.configuration.ConfigurationException
     */
    public static CompositeConfiguration parsePropertyConfig(File confDir,
            String[] fileName) throws ConfigurationException {
        CompositeConfiguration config = new CompositeConfiguration();
        for (int i = 0; i < fileName.length; i++) {
            File configFile = new File(confDir, fileName[i]);
            if (configFile.exists()) {
                LOG.info("Loading config from " + configFile.getAbsolutePath());
                PropertiesConfiguration thisConf = new PropertiesConfiguration(
                        configFile);
                // clear keys to be added by this conf file
                HashSet<String> removeKeys = new HashSet<String>();
                Iterator it = config.getKeys();
                while (it.hasNext()) {
                    String key = (String) (it.next());
                    if (thisConf.containsKey(key)) {
                        removeKeys.add(key);
                    }
                }
                for (String key: removeKeys) {
                    config.clearProperty(key);
                }
                // add this conf file
                config.addConfiguration(thisConf);
            } else
                LOG.warn("Ignoring config file " + configFile.getAbsolutePath()
                        + " because it does not exist");
        }
        // allow users to override on command line
        config.addConfiguration(new SystemConfiguration());
        return config;
    }

    /**
     * Parse the xml-configure file
     * 
     * @param confDir
     * @param fileName
     * @return
     * @throws org.apache.commons.configuration.ConfigurationException
     */
    public static CompositeConfiguration parseXmlConfig(File confDir,
            String[] fileName) throws ConfigurationException {
        CompositeConfiguration config = new CompositeConfiguration();
        for (int i = 0; i < fileName.length; i++) {
            File xmlFile = new File(confDir, fileName[i]);
            if (xmlFile.exists()) {
                LOG.info("Loading config from " + xmlFile.getAbsolutePath());
                XMLConfiguration thisConf = new XMLConfiguration(xmlFile);
                // clear keys to be added by this conf file
                HashSet<String> removeKeys = new HashSet<String>();
                Iterator it = config.getKeys();
                while (it.hasNext()) {
                    String key = (String) (it.next());
                    if (thisConf.containsKey(key)) {
                        removeKeys.add(key);
                    }
                }
                for (String key: removeKeys) {
                    config.clearProperty(key);
                }
                // add this conf file
                config.addConfiguration(thisConf);
            } else
                LOG.warn("Ignoring config file " + xmlFile.getAbsolutePath()
                        + " because it does not exist.");
        }
        return config;
    }



}
