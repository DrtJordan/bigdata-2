package com.netease.weblogOffline.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class UrlPvUvWritable implements Writable{
	private   String url="";
	private int pv = 0;

	private int uv = 0;

	public UrlPvUvWritable(String url,int pv, int uv) {
		super();
		this.pv = pv;
		this.uv = uv;
		this.url = url;
	}
	public UrlPvUvWritable() {

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
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.url);
		out.writeInt(this.pv);
		out.writeInt(this.uv);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.url=in.readUTF();
		this.pv = in.readInt();
		this.uv = in.readInt();

	}
	
}
