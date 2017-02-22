package com.netease.weblogOffline.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class MultiStatisticResultWritable implements Writable {
	
	private Map<Text,StatisticResultWritable> dataMap = new TreeMap<Text, StatisticResultWritable>();

	public MultiStatisticResultWritable() {}
	
	public Map<Text, StatisticResultWritable> getDataMap() {
		return dataMap;
	}
	
	public void add(MultiStatisticResultWritable msrw){
		for(Entry<Text, StatisticResultWritable> entry : msrw.getDataMap().entrySet()){
			dataMap.put(new Text(entry.getKey()), new StatisticResultWritable(entry.getValue()));
		}
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		dataMap.clear();
		int size = in.readInt();
		while(size-- > 0){
			Text k = new Text();
			k.readFields(in);
			StatisticResultWritable v = new StatisticResultWritable();
			v.readFields(in);
			dataMap.put(k, v);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(dataMap.size());
		for(Entry<Text, StatisticResultWritable> entry : dataMap.entrySet()){
			entry.getKey().write(out);
			entry.getValue().write(out);
		}
	}
	
	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		for(Entry<Text, StatisticResultWritable> entry : dataMap.entrySet()){
			sb.append(entry.getKey().toString()).append(":").append(entry.getValue()).append(";");
		}
		return sb.toString();
	}
	
}
