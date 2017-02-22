package com.netease.weblogOffline.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class StringLongSemiHash implements WritableComparable<StringLongSemiHash> {
	
	private String first;
	
	private long second;
	
	public StringLongSemiHash() {}
	
	public StringLongSemiHash(String first, long second) {
		this.first = first;
		this.second = second;
	}
	
	@Override
	public String toString() {
		return first + "," + second;
	}

	@Override
	public int hashCode(){
		return first.hashCode();
	}
	
	@Override
	public int compareTo(StringLongSemiHash o) {
		if(null == o){
			return 1;
		}
		
		int res = first.compareTo(o.first);
		
		if(0 == res){
			if(second > o.second){
				res = 1;
			}else if(second == o.second){
				res = 0;
			}else{
				res = -1;
			}
		}
		
		return res;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first = in.readUTF();
		second = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(first);
		out.writeLong(second);
	}

	public String getFirst() {
		return first;
	}

	public void setFirst(String first) {
		this.first = first;
	}

	public long getSecond() {
		return second;
	}

	public void setSecond(long second) {
		this.second = second;
	}
}
