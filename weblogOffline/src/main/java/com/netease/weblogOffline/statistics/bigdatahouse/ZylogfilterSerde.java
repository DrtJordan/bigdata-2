package com.netease.weblogOffline.statistics.bigdatahouse;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.netease.weblogOffline.common.zylogfilter.ZylogFilterUtils;

@SuppressWarnings("deprecation")
public class ZylogfilterSerde implements Deserializer {
	private static List<String> structFieldNames = new ArrayList<String>();
	private static List<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();

	static {
		for(String column : ZylogFilterUtils.getColumns()){
			structFieldNames.add(column);
			structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		}
	}

	public Object deserialize(Writable writable) throws SerDeException {
		List<Object> result = new ArrayList<Object>();
		try {
			Text rowText = (Text) writable;
			String[] items = rowText.toString().split("\t");
			if(items != null&& items.length>0){
				for (int i=0;i<items.length;i++){
					result.add(items[i]);
				}
			}
		} catch (Exception localException) {}
		
		return result;
	}

	public ObjectInspector getObjectInspector() throws SerDeException {
		return ObjectInspectorFactory.getStandardStructObjectInspector(
				structFieldNames, structFieldObjectInspectors);
	}

	public SerDeStats getSerDeStats() {
		return null;
	}

	public void initialize(Configuration job, Properties arg1)
			throws SerDeException {
	}
}



