package com.netease.weblogOffline.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.netease.weblogOffline.utils.HadoopUtils;

public class MultiStatisticResultWrapWritable implements Writable {
	
	private MultiStatisticResultWritable msr =new MultiStatisticResultWritable();
	
	private Map<String, String> conf = new HashMap<>();
	
	@Override
	public void readFields(DataInput in) throws IOException {
		msr = new MultiStatisticResultWritable();
		msr.readFields(in);
		int size = in.readInt();
		conf = new HashMap<>();
		while(size-- > 0){
			conf.put(in.readUTF().toString(), in.readUTF().toString());
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		msr.write(out);
		out.writeInt(conf.size());
		for(Entry<String, String> entry : conf.entrySet()){
			HadoopUtils.writeString(out, entry.getKey());
			HadoopUtils.writeString(out, entry.getValue());
		}
	}
	
	@Override
	public String toString(){
		StringBuilder sb= new StringBuilder();
		
		for(Entry<String, String> entry : conf.entrySet()){
			sb.append(entry.getKey().toString()).append(":").append(entry.getValue()).append(";");
		}
		sb.append(msr.toString());
		return sb.toString();
	}
	
	
//	@Override
//	public String toString(){
//		String[] keys = {"base_app_all","share_app_all","back_app_all","genTie_app_all","contentUp_all_all"
//				,"contentDown_all_all","sessionCount_app_all","base_www_all","share_www_all","back_www_all",
//				"genTie_www_all",
//				"contentUp_all_all"
//				,"contentDown_all_all","sessionCount_www_all"};
//		StringBuilder sb= new StringBuilder();
//       for (String key :keys){
//    	   int pv = 0;
//    	   int uv = 0;
//    	    StatisticResultWritable s = this.msr.getDataMap().get(new Text(key));
//    	    if(s != null){
//    	    	pv = s.getPv();
//    	    	uv = s.getUv();
//    	    }
//    	    sb.append(pv + "," + uv).append("\t");
//       }
//		return sb.toString().trim();
//	}
	public MultiStatisticResultWritable getMsr() {
		return msr;
	}

	public void setMsr(MultiStatisticResultWritable msr) {
		this.msr = msr;
	}

	public Map<String, String> getConf() {
		return conf;
	}

	public void setConf(Map<String, String> conf) {
		this.conf = conf;
	}
	public void add(Map<String, String> conf){
		for(Entry<String, String> entry : conf.entrySet()){
			this.conf.put(new String(entry.getKey()), new String(entry.getValue()));
		}
	}
}
