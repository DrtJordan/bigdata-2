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

import com.netease.weblogCommon.utils.TextUtils;



/**
 * 跟帖日志解析  供hive建表
 */
@SuppressWarnings("deprecation")
public class GenTieSerde implements Deserializer {

	private static List<String> structFieldNames = new ArrayList<String>();
	private static List<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();

	static {
		for(String column : GenTieUtils.getColumns()){
			structFieldNames.add(column);
			structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
		}
	}


	public Object deserialize(Writable writable) throws SerDeException {
		List<Object> result = new ArrayList<Object>();
		try {
			Text rowText = (Text) writable;
			String[] strs = rowText.toString().split(",");
			String[] column = GenTieUtils.getColumns();
			for(int i = 0 ;i <column.length;i++){
				result.add(TextUtils.notNullStr(strs[i],GenTieUtils.defNullStr));
			}
		 } catch (Exception localException1) {
		 }

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
