package com.netease.weblogOffline.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Writable;

public class HashMapStringIntegerWritable  implements Writable {

	
	HashMap<String,Integer> hm = new HashMap<String,Integer>();

	public HashMapStringIntegerWritable(){
		
	}



	public HashMapStringIntegerWritable(HashMap<String, Integer> hm) {

		this.hm = hm;
	}



	public HashMap<String, Integer> getHm() {
		return hm;
	}



	public void setHm(HashMap<String, Integer> hm) {
		this.hm = hm;
	}



	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
	  out.writeInt(this.hm.size());
	  
	  for (String s :hm.keySet()){
		  out.writeUTF(s);
		  out.writeInt(hm.get(s));
	   }
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		HashMap<String,Integer> hm2 = new HashMap<String,Integer>();
		int size =in.readInt();
		for (int i=0;i<size;i++){
			String key = in.readUTF();
			Integer value = in.readInt();
			hm2.put(key, value);
		}
		this.hm = hm2;
	}
	
	  public int length() {
		    return this.hm.size();
		  }
		  
		  @Override
		  public String toString() {
		    return hm.toString();
		  }
		  
		  @Override
		  public int hashCode() {
		    return hm.hashCode();
		  }

}
