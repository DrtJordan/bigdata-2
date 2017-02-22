package com.netease.weblogOffline.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class StatisticResultWritable implements Writable {
	
	private int pv;
	
	private int uv;
	
	public StatisticResultWritable() {
		this(0, 0);
	}
	
	public StatisticResultWritable(StatisticResultWritable srw) {
		this(srw.getPv(), srw.getUv());
	}
	
	public StatisticResultWritable(int pv, int uv) {
		this.pv = pv;
		this.uv = uv;
	}

	public int increasePV(){
		return increasePV(1);
	}
	
	public int increasePV(int n){
		this.pv += n;
		return this.pv;
	}
	
	public int increaseUV(){
		return increaseUV(1);
	}
	
	public int increaseUV(int n){
		this.uv += n;
		return this.uv;
	}
	
	public void reSet(){
		this.pv = 0;
		this.uv = 0;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.pv = in.readInt();
		this.uv = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.pv);
		out.writeInt(this.uv);
	}
	
	public String toString(){
		return this.pv + "," + this.uv;
	}
	
	public int getPv() {
		return pv;
	}

	public void setPv(int pv) {
		this.pv = pv;
	}

	public int getUv() {
		return uv;
	}

	public void setUv(int uv) {
		this.uv = uv;
	}
}
